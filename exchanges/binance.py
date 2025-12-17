import aiohttp
from .base import Exchange
import asyncio
import json
import os
import logging
import time

CACHE_FILE = "binance_intervals.json"
MIN_INTERVAL_H = 1.0
MAX_INTERVAL_H = 8.0


class Binance(Exchange):
    def __init__(self):
        super().__init__("Binance", "https://fapi.binance.com")
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.interval_cache: dict[str, float] = self._load_cache()
        self._cache_dirty = False
        self.logger = logging.getLogger("Binance")
        self.last_next_file = "binance_last_next.json"
        self.last_next_funding_map: dict[str, int] = self._load_last_next()
        self._last_next_dirty = False
        self.catchup_flags: dict[str, bool] = {}

    # =============================
    # Cache & symbol helpers
    # =============================

    def _normalize_symbol(self, symbol: str) -> str:
        """统一处理为大写，避免缓存 miss / API 不一致。"""
        return symbol.upper()

    def _load_cache(self) -> dict[str, float]:
        if not os.path.exists(CACHE_FILE):
            return {}
        with open(CACHE_FILE, "r") as f:
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
        with open(CACHE_FILE, "w") as f:
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

    def _log_cache_fallback(self, symbol: str, reason: str) -> None:
        """日志记录：interval 失败时回退缓存"""
        cache_val = self._get_cached_interval(symbol)
        self.logger.warning(
            "Binance interval fallback to cache for %s: %s (cached=%s)",
            symbol,
            reason,
            cache_val,
        )

    def _next_hour_ts_ms(self, now_ms: int | None = None) -> int:
        """返回下一个整点的时间戳（毫秒）"""
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        sec = now_ms // 1000
        next_hour_sec = ((sec // 3600) + 1) * 3600
        return next_hour_sec * 1000

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

    def _closest_standard(self, hrs: float) -> float:
        candidates = (1.0, 4.0, 8.0)
        return min(candidates, key=lambda h: abs(h - hrs))

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

    def _infer_interval_from_payload(self, symbol: str, item: dict, cached: float | None = None) -> float | None:
        """
        只使用连续两次 nextFundingTime 的差来推断 interval。
        若本次 nextFundingTime 与上次相同，直接用缓存。
        """
        try:
            nxt = item.get("nextFundingTime")
            if nxt is None:
                return cached
            nxt = int(nxt)

            prev_nxt = self.last_next_funding_map.get(symbol)
            # 始终记录最新的 nextFundingTime
            self.last_next_funding_map[symbol] = nxt
            self._last_next_dirty = True

            # 没有上次记录，或 next 未变化，则保持缓存
            if prev_nxt is None or prev_nxt == nxt:
                return cached

            diff_prev = abs(nxt - prev_nxt)
            if diff_prev <= 0:
                return cached

            hrs = diff_prev / 3_600_000
            hrs = max(MIN_INTERVAL_H, min(MAX_INTERVAL_H, hrs))

            # 正常区间：接近 1/4/8
            snapped = self._closest_standard(self._snap_hours(hrs))
            close_enough = abs(snapped - hrs) < 0.25

            # 上一次出现异常，且这次恢复正常，则清除追赶标志
            if self.catchup_flags.get(symbol) and close_enough:
                self.catchup_flags[symbol] = False
                return snapped

            if close_enough and hrs <= 8.25:
                return snapped

            # 异常：跳过/超 8h，报警并进入追赶模式，暂用缓存
            self.catchup_flags[symbol] = True
            self.logger.warning(
                "Binance interval anomaly for %s: delta_hours=%.3f (prev_next=%s, next=%s), using cached=%s",
                symbol,
                hrs,
                prev_nxt,
                nxt,
                cached,
            )
            return cached
        except Exception:
            return cached

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
                cached = self._get_cached_interval(norm_symbol)
                inferred = self._infer_interval_from_payload(norm_symbol, data, cached)
                if inferred is not None:
                    interval_hours = inferred
                    self._set_cached_interval(norm_symbol, interval_hours)
                elif cached is not None:
                    interval_hours = cached
                else:
                    interval_hours = MAX_INTERVAL_H

                # 单次查询也顺便把 cache 刷到磁盘（可按需去掉，减少 IO）
                self._save_cache()
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

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")

                data = await resp.json()
                semaphore = asyncio.Semaphore(3)

                async def enrich(item: dict) -> dict:
                    async with semaphore:
                        symbol = self._normalize_symbol(item["symbol"])
                        nextFundingTime = (
                            int(item.get("nextFundingTime", 0))
                            if item.get("nextFundingTime") else None
                        )
                        cached = self._get_cached_interval(symbol)
                        inferred = self._infer_interval_from_payload(symbol, item, cached)
                        if inferred is not None:
                            hrs = inferred
                            self._set_cached_interval(symbol, hrs)
                        elif cached is not None:
                            hrs = cached
                        else:
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
                self._save_last_next()
                return results
