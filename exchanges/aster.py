import aiohttp
import asyncio
import json
import os
from .base import Exchange
import logging

MIN_INTERVAL_H = 1.0
MAX_INTERVAL_H = 8.0


class Aster(Exchange):
    def __init__(self):
        super().__init__("Aster", "https://fapi.asterdex.com")
        self.cache_file = "aster_intervals.json"
        self.invalid_cache_file = "aster_invalid_symbols.json"
        self.interval_cache: dict[str, float] = self._load_cache()
        self.invalid_symbol_cache: set[str] = self._load_invalid_cache()
        self._cache_dirty = False
        self._invalid_cache_dirty = False
        self.logger = logging.getLogger("Aster")

    def _load_cache(self) -> dict[str, float]:
        """
        读取本地缓存，过滤掉异常值。
        """
        if not os.path.exists(self.cache_file):
            return {}
        with open(self.cache_file, "r") as f:
            data = json.load(f)
        result: dict[str, float] = {}
        for k, v in data.items():
            val = float(v)
            if MIN_INTERVAL_H <= val <= MAX_INTERVAL_H:
                result[k.upper()] = val
        return result

    def _save_cache(self) -> None:
        if not self._cache_dirty:
            return
        with open(self.cache_file, "w") as f:
            json.dump(self.interval_cache, f, indent=2, sort_keys=True)
        self._cache_dirty = False

    def _load_invalid_cache(self) -> set[str]:
        if not os.path.exists(self.invalid_cache_file):
            return set()
        with open(self.invalid_cache_file, "r") as f:
            data = json.load(f)
        return {str(s).upper() for s in data if isinstance(s, str)}

    def _save_invalid_cache(self) -> None:
        if not self._invalid_cache_dirty:
            return
        with open(self.invalid_cache_file, "w") as f:
            json.dump(sorted(self.invalid_symbol_cache), f, indent=2)
        self._invalid_cache_dirty = False

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

    def _log_cache_fallback(self, symbol: str, reason: str) -> None:
        self.logger.warning("Aster interval fallback to cache for %s: %s", symbol, reason)

    def _add_invalid_symbol(self, symbol: str) -> None:
        sym = self._normalize_symbol(symbol)
        if sym not in self.invalid_symbol_cache:
            self.invalid_symbol_cache.add(sym)
            self._invalid_cache_dirty = True

    # ============ 核心 interval 计算逻辑 ============

    async def _fetch_interval_hours(
        self,
        symbol: str,
        session: aiohttp.ClientSession,
        nextFundingTime: int | None = None,
    ) -> float | None:
        """
        根据 /fapi/v1/fundingRate 推断 funding interval（小时）。
        优先单查，失败时才回退到本地缓存。
        """
        norm_symbol = self._normalize_symbol(symbol)

        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {"symbol": norm_symbol, "limit": 2}

        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    self._log_cache_fallback(norm_symbol, f"status {resp.status}")
                    return self._get_cached_interval(norm_symbol)

                data = await resp.json()
                if not isinstance(data, list) or len(data) == 0:
                    self._log_cache_fallback(norm_symbol, "empty fundingRate response")
                    return self._get_cached_interval(norm_symbol)

                # 提取 fundingTime（毫秒时间戳），并统一按时间降序（最新在前）
                funding_times = []
                for item in data:
                    t = item.get("fundingTime")
                    if isinstance(t, int):
                        funding_times.append(t)
                funding_times.sort(reverse=True)

                if not funding_times:
                    self._log_cache_fallback(norm_symbol, "no fundingTime in response")
                    return self._get_cached_interval(norm_symbol)

                # 优先用 nextFundingTime 与最近一次 fundingTime 的差
                if nextFundingTime is not None:
                    t_last = funding_times[0]
                    hrs = abs(nextFundingTime - t_last) / 3_600_000
                else:
                    # fallback: 用最近两次 fundingTime 的差
                    if len(funding_times) < 2:
                        self._log_cache_fallback(norm_symbol, "not enough fundingTime entries")
                        return self._get_cached_interval(norm_symbol)
                    t1, t2 = funding_times[0], funding_times[1]
                    hrs = abs(t1 - t2) / 3_600_000

                hrs = self._snap_hours(hrs)
                hrs = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, hrs))
                self._set_cached_interval(norm_symbol, hrs)
                return hrs
        except Exception as e:
            self._log_cache_fallback(norm_symbol, f"exception {e}")
            return self._get_cached_interval(norm_symbol)

    async def _is_symbol_valid(
        self, symbol: str, session: aiohttp.ClientSession
    ) -> bool:
        """
        先单查 premiumIndex。只有明确的 400/Invalid symbol 才写入 invalid cache。
        其它失败则回退到已有的 invalid cache（若存在）判定，避免误标记。
        """
        norm_symbol = self._normalize_symbol(symbol)

        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": norm_symbol}
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                # 若之前在 invalid cache，修正回有效
                if norm_symbol in self.invalid_symbol_cache:
                    self.invalid_symbol_cache.discard(norm_symbol)
                    self._invalid_cache_dirty = True
                return True

            text = await resp.text()
            if resp.status == 400 or "Invalid symbol" in text:
                self._add_invalid_symbol(norm_symbol)
                return False

            # 其它失败：不更新 invalid cache，只用已有缓存判断
            return norm_symbol not in self.invalid_symbol_cache

    # ============ 对外接口 ============

    async def get_funding_rate(self, symbol: str) -> dict:
        norm_symbol = self._normalize_symbol(symbol)
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": norm_symbol}

        async with aiohttp.ClientSession() as session:
            valid = await self._is_symbol_valid(norm_symbol, session)
            if not valid:
                self._save_invalid_cache()
                raise Exception(f"Aster symbol invalid: {norm_symbol}")

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

                async def enrich(item: dict) -> dict | None:
                    async with semaphore:
                        symbol = self._normalize_symbol(item["symbol"])

                        # 先单查接口；若明确 invalid 就过滤，否则继续
                        valid = await self._is_symbol_valid(symbol, session)
                        if not valid:
                            return None

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
                self._save_invalid_cache()
                return [r for r in results if r is not None]
