import aiohttp
import asyncio
import json
import os
from .base import Exchange
import logging
import time

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
        self.last_next_file = "aster_last_next.json"
        self.last_next_funding_map: dict[str, int] = self._load_last_next()
        self._last_next_dirty = False
        self.catchup_flags: dict[str, bool] = {}

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

    def _load_last_next(self) -> dict[str, int]:
        if not os.path.exists(self.last_next_file):
            return {}
        with open(self.last_next_file, "r") as f:
            data = json.load(f)
        result: dict[str, int] = {}
        for k, v in data.items():
            try:
                result[k.upper()] = int(v)
            except Exception:
                continue
        return result

    def _save_last_next(self) -> None:
        if not self._last_next_dirty:
            return
        with open(self.last_next_file, "w") as f:
            json.dump(self.last_next_funding_map, f, indent=2, sort_keys=True)
        self._last_next_dirty = False

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

    def _closest_standard(self, hrs: float) -> float:
        candidates = (1.0, 4.0, 8.0)
        return min(candidates, key=lambda h: abs(h - hrs))

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

    def _infer_interval(self, symbol: str, item: dict) -> float | None:
        """
        使用连续两次 nextFundingTime 的差推断 interval。
        next 未推进或无上次记录时，返回 None（用缓存）。
        异常跳变（>8h 或偏离 1/4/8 很多）进入 catchup：保留缓存并报警。
        """
        try:
            nxt = item.get("nextFundingTime")
            if nxt is None:
                return None
            nxt = int(nxt)

            prev_nxt = self.last_next_funding_map.get(symbol)
            # 始终记录最新 nextFundingTime
            self.last_next_funding_map[symbol] = nxt
            self._last_next_dirty = True

            if prev_nxt is None or prev_nxt == nxt:
                return None

            diff = nxt - prev_nxt
            if diff <= 0:
                return None

            hrs = diff / 3_600_000
            hrs = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, hrs))
            snapped = self._closest_standard(self._snap_hours(hrs))
            close_enough = abs(snapped - hrs) < 0.25 and hrs <= 8.25

            # catchup 恢复：上次异常，这次正常
            if self.catchup_flags.get(symbol) and close_enough:
                self.catchup_flags[symbol] = False
                return snapped

            if close_enough:
                return snapped

            # 异常：超出合理范围，进入追赶，保留缓存
            self.catchup_flags[symbol] = True
            self.logger.warning(
                "Aster interval anomaly for %s: delta_hours=%.3f (prev_next=%s, next=%s), keeping cache",
                symbol,
                hrs,
                prev_nxt,
                nxt,
            )
            return None
        except Exception:
            return None

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
                cached = self._get_cached_interval(norm_symbol)
                inferred = self._infer_interval(norm_symbol, data)
                if inferred is not None:
                    interval_hours = inferred
                    self._set_cached_interval(norm_symbol, interval_hours)
                elif cached is not None:
                    interval_hours = cached
                else:
                    interval_hours = MAX_INTERVAL_H

                self._save_last_next()

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
                        cached = self._get_cached_interval(symbol)
                        inferred = self._infer_interval(symbol, item)
                        if inferred is not None:
                            interval_hours = inferred
                            self._set_cached_interval(symbol, interval_hours)
                        elif cached is not None:
                            interval_hours = cached
                        else:
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
                self._save_last_next()
                self._save_invalid_cache()
                return [r for r in results if r is not None]
