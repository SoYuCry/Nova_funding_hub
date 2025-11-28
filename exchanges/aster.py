import aiohttp
import asyncio
import json
import os
from .base import Exchange

MIN_INTERVAL_H = 1.0
MAX_INTERVAL_H = 8.0


class Aster(Exchange):
    def __init__(self):
        super().__init__("Aster", "https://fapi.asterdex.com")
        self.cache_file = "aster_intervals.json"
        self.interval_cache: dict[str, float] = self._load_cache()
        self._cache_dirty = False

    def _load_cache(self) -> dict[str, float]:
        """
        读取本地缓存，过滤掉异常值。
        """
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "r") as f:
                    data = json.load(f)
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
            with open(self.cache_file, "w") as f:
                json.dump(self.interval_cache, f, indent=2, sort_keys=True)
            self._cache_dirty = False
        except Exception:
            pass

    # ============ 工具函数 ============

    def _normalize_symbol(self, symbol: str) -> str:
        return symbol.upper()

    def _snap_hours(self, hrs: float) -> float:
        """
        把接近 8/4/1 小时的值 snap 到整数，避免浮点误差。
        """
        if 7.9999 < hrs < 8.001:
            return 8.0
        if 3.9999 < hrs < 4.001:
            return 4.0
        if 0.9999 < hrs < 1.001:
            return 1.0
        return hrs

    def _get_cached_interval(self, symbol: str) -> float | None:
        sym = self._normalize_symbol(symbol)
        hrs = self.interval_cache.get(sym)
        if hrs is None:
            return None
        if hrs < MIN_INTERVAL_H or hrs > MAX_INTERVAL_H:
            return None
        return float(hrs)

    def _set_cached_interval(self, symbol: str, hrs: float) -> None:
        sym = self._normalize_symbol(symbol)
        val = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, float(hrs)))
        self.interval_cache[sym] = val
        self._cache_dirty = True

    # ============ 核心 interval 计算逻辑 ============

    async def _fetch_interval_hours(
        self,
        symbol: str,
        session: aiohttp.ClientSession,
        nextFundingTime: int | None = None,
    ) -> float | None:
        """
        根据 /fapi/v1/fundingRate 推断 funding interval（小时）。
        每次都从接口获取，不使用缓存。
        """
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

                # 提取 fundingTime（毫秒时间戳），并统一按时间降序（最新在前）
                funding_times = []
                for item in data:
                    t = item.get("fundingTime")
                    if isinstance(t, int):
                        funding_times.append(t)
                funding_times.sort(reverse=True)

                if not funding_times:
                    return None

                # 优先用 nextFundingTime 与最近一次 fundingTime 的差
                if nextFundingTime is not None:
                    t_last = funding_times[0]
                    hrs = abs(nextFundingTime - t_last) / 3_600_000
                else:
                    # fallback: 用最近两次 fundingTime 的差
                    if len(funding_times) < 2:
                        return self._get_cached_interval(norm_symbol)
                    t1, t2 = funding_times[0], funding_times[1]
                    hrs = abs(t1 - t2) / 3_600_000

                hrs = self._snap_hours(hrs)
                hrs = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, hrs))
                self._set_cached_interval(norm_symbol, hrs)
                return hrs

        except Exception:
            return self._get_cached_interval(norm_symbol)

    # ============ 对外接口 ============

    async def get_funding_rate(self, symbol: str) -> dict:
        norm_symbol = self._normalize_symbol(symbol)
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": norm_symbol}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"Aster API error: {response.status}")

                data = await response.json()
                nextFundingTime = (
                    int(data.get("nextFundingTime", 0))
                    if data.get("nextFundingTime")
                    else None
                )

                # 理论上有 symbol 参数就应该返回 dict，这里多一层防御
                if isinstance(data, list):
                    for item in data:
                        if item.get("symbol") == norm_symbol:
                            data = item
                            break

                interval_hours = await self._fetch_interval_hours(
                    norm_symbol, session, nextFundingTime
                )
                if interval_hours is None:
                    interval_hours = self._get_cached_interval(norm_symbol)
                if interval_hours is None:
                    interval_hours = MAX_INTERVAL_H

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

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Aster API error: {response.status}")

                data = await response.json()
                semaphore = asyncio.Semaphore(5)

                async def enrich(item: dict) -> dict:
                    async with semaphore:
                        symbol = self._normalize_symbol(item["symbol"])
                        nextFundingTime = (
                            int(item.get("nextFundingTime", 0))
                            if item.get("nextFundingTime")
                            else None
                        )
                        interval_hours = await self._fetch_interval_hours(
                            symbol, session, nextFundingTime
                        )
                        if interval_hours is None:
                            interval_hours = self._get_cached_interval(symbol)
                        if interval_hours is None:
                            interval_hours = MAX_INTERVAL_H
                        return {
                            "exchange": self.name,
                            "symbol": symbol,
                            "rate": float(item["lastFundingRate"]),
                            "timestamp": int(item["time"]),
                            "nextFundingTime": nextFundingTime,
                            "interval_hours": interval_hours,
                        }

                results = await asyncio.gather(*(enrich(item) for item in data))
                self._save_cache()
                return results
