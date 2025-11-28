import aiohttp
from .base import Exchange
import asyncio
import json
import os

CACHE_FILE = "binance_intervals.json"
MIN_INTERVAL_H = 1.0
MAX_INTERVAL_H = 8.0


class Binance(Exchange):
    def __init__(self):
        super().__init__("Binance", "https://fapi.binance.com")
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.interval_cache: dict[str, float] = self._load_cache()
        self._cache_dirty = False

    # =============================
    # Cache & symbol helpers
    # =============================

    def _normalize_symbol(self, symbol: str) -> str:
        """统一处理为大写，避免缓存 miss / API 不一致。"""
        return symbol.upper()

    def _load_cache(self) -> dict[str, float]:
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r") as f:
                    data = json.load(f)
                # 保险起见，把 key 都 upper 一遍
                result: dict[str, float] = {}
                for k, v in data.items():
                    try:
                        val = float(v)
                        if MIN_INTERVAL_H <= val <= MAX_INTERVAL_H:
                            result[k.upper()] = val
                    except Exception:
                        continue
                return result
        except Exception:
            pass
        return {}

    def _save_cache(self) -> None:
        if not self._cache_dirty:
            return
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(self.interval_cache, f, indent=2, sort_keys=True)
            self._cache_dirty = False
        except Exception:
            # 写入失败只影响缓存持久化，不要影响主流程
            pass

    def _get_cached_interval(self, symbol: str) -> float | None:
        """从缓存里拿 interval（小时），没有则返回 None。"""
        sym = self._normalize_symbol(symbol)
        hrs = self.interval_cache.get(sym)
        if hrs is None:
            return None
        if hrs < MIN_INTERVAL_H or hrs > MAX_INTERVAL_H:
            return None
        return float(hrs)

    def _set_cached_interval(self, symbol: str, hrs: float) -> None:
        """写缓存并标记 dirty。"""
        sym = self._normalize_symbol(symbol)
        val = float(hrs)
        val = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, val))
        self.interval_cache[sym] = val
        self._cache_dirty = True

    # =============================
    # 数据处理 helpers
    # =============================

    def _snap_hours(self, hrs: float) -> float:
        """
        把接近 8/4/1 小时的数 snap 到整数，避免精度误差导致看起来是 7.99998 之类。
        """
        if 7.9999 < hrs < 8.001:
            return 8.0
        if 3.9999 < hrs < 4.001:
            return 4.0
        if 0.9999 < hrs < 1.001:
            return 1.0
        return hrs

    def _extract_funding_times(self, data: list[dict]) -> list[int]:
        """
        从 fundingRate 接口返回的数据中抽出 fundingTime 列表（毫秒时间戳）。
        """
        times: list[int] = []
        for item in data:
            t = item.get("fundingTime")
            if isinstance(t, int):
                times.append(t)
        # Binance 返回的列表有时是升序，这里统一降序（最新在前）
        return sorted(times, reverse=True)

    # =============================
    # 核心 interval 计算逻辑
    # =============================

    async def _fetch_interval_hours(
        self,
        symbol: str,
        session: aiohttp.ClientSession,
        nextFundingTime: int | None = None,
    ) -> float | None:
        """
        根据 Binance fundingRate 接口推断 funding interval（小时）。
        优先使用 nextFundingTime 与最近一次 fundingTime 的差值，
        否则 fallback 到最近两次 fundingTime 的差值。
        """
        # 先查缓存
        cached = self._get_cached_interval(symbol)
        if cached is not None:
            return cached

        norm_symbol = self._normalize_symbol(symbol)
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {"symbol": norm_symbol, "limit": 2}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None

                data = await resp.json()
                if not isinstance(data, list) or len(data) == 0:
                    return None

                funding_times = self._extract_funding_times(data)
                if not funding_times:
                    return None

                hrs: float | None = None

                # 有 nextFundingTime 时优先用它：next - 最近的一次 fundingTime
                if nextFundingTime is not None:
                    # funding_times 已经按时间降序排序，index 0 为最新一次
                    t_last = funding_times[0]
                    hrs = abs(nextFundingTime - t_last) / 3_600_000
                # 否则，退化成用最近两次 fundingTime 的间隔
                elif len(funding_times) >= 2:
                    t1, t2 = funding_times[0], funding_times[1]
                    hrs = abs(t1 - t2) / 3_600_000

                if hrs is None:
                    return self._get_cached_interval(norm_symbol)

                hrs = self._snap_hours(hrs)
                hrs = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, hrs))
                self._set_cached_interval(norm_symbol, hrs)
                return hrs

        except Exception:
            return None

    # =============================
    # 对外接口
    # =============================

    async def get_funding_rate(self, symbol: str) -> dict:
        norm_symbol = self._normalize_symbol(symbol)
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": norm_symbol}

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")

                data = await resp.json()

                nextFundingTime = (
                    int(data.get("nextFundingTime", 0))
                    if data.get("nextFundingTime") else None
                )

                interval_hours = await self._fetch_interval_hours(
                    norm_symbol, session, nextFundingTime
                )
                if interval_hours is None:
                    interval_hours = self._get_cached_interval(norm_symbol)
                if interval_hours is None:
                    interval_hours = MAX_INTERVAL_H

                # 单次查询也顺便把 cache 刷到磁盘（可按需去掉，减少 IO）
                self._save_cache()

                return {
                    "exchange": self.name,
                    "symbol": norm_symbol,
                    "rate": float(data["lastFundingRate"]),
                    "timestamp": int(data["time"]),
                    "nextFundingTime": nextFundingTime,
                    "interval_hours": interval_hours,
                }

    async def get_all_funding_rates(self) -> list[dict]:
        url = f"{self.base_url}/fapi/v1/premiumIndex"

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")

                data = await resp.json()
                semaphore = asyncio.Semaphore(5)

                async def enrich(item: dict) -> dict:
                    async with semaphore:
                        symbol = self._normalize_symbol(item["symbol"])
                        nextFundingTime = (
                            int(item.get("nextFundingTime", 0))
                            if item.get("nextFundingTime") else None
                        )
                        hrs = await self._fetch_interval_hours(
                            symbol, session, nextFundingTime
                        )
                        if hrs is None:
                            hrs = self._get_cached_interval(symbol)
                        if hrs is None:
                            hrs = MAX_INTERVAL_H
                        return {
                            "exchange": self.name,
                            "symbol": symbol,
                            "rate": float(item["lastFundingRate"]),
                            "timestamp": int(item["time"]),
                            "nextFundingTime": nextFundingTime,
                            "interval_hours": hrs,
                        }

                results = await asyncio.gather(*(enrich(item) for item in data))
                self._save_cache()
                return results
