import aiohttp
import time
import asyncio
from datetime import datetime
import logging
from .base import Exchange


class Backpack(Exchange):
    def __init__(self):
        super().__init__("Backpack", "https://api.backpack.exchange")
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.logger = logging.getLogger("Backpack")

    async def _get_markets(self):
        url = f"{self.base_url}/api/v1/markets"
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Backpack API error: {resp.status}")
                return await resp.json()

    async def _fetch_latest_rate(self, symbol: str) -> dict | None:
        """
        Use fundingRates history endpoint to get last interval.
        """
        url = f"{self.base_url}/api/v1/fundingRates"
        params = {"symbol": symbol, "limit": 1}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if isinstance(data, list) and data:
                    return data[0]
                return None

    def _api_symbol(self, symbol: str) -> str:
        base = symbol.upper().replace("USDT", "").replace("USDC", "").replace("_", "").replace("-", "").replace("/", "")
        return f"{base}_USDC_PERP"

    def _parse_ts(self, ts_val, default_ms: int) -> int:
        if ts_val is None:
            return default_ms
        try:
            return int(ts_val)
        except Exception:
            pass
        if isinstance(ts_val, str):
            try:
                dt = datetime.fromisoformat(ts_val)
                return int(dt.timestamp() * 1000)
            except Exception:
                return default_ms
        return default_ms

    async def get_funding_rate(self, symbol: str) -> dict:
        api_symbol = self._api_symbol(symbol)
        markets = await self._get_markets()
        interval_hours = None
        interval_ms = None
        for m in markets:
            if m.get("symbol", "").upper() == api_symbol:
                interval_ms = m.get("fundingInterval")
                if interval_ms:
                    interval_hours = float(interval_ms) / 3600_000
                break
        # Try history first
        latest = await self._fetch_latest_rate(api_symbol)
        ts_now = int(time.time() * 1000)

        if latest:
            base_symbol = api_symbol.split("_")[0]
            unified_symbol = f"{base_symbol}USDT"
            return {
                "exchange": self.name,
                "symbol": unified_symbol,
                "rate": float(latest.get("fundingRate", 0.0)),
                "timestamp": self._parse_ts(latest.get("intervalEndTimestamp"), ts_now),
                "interval_hours": interval_hours,
                "fundingInterval": interval_ms,
            }

        # Fallback: pull markets info for fundingRate if present
        for m in markets:
            if m.get("symbol", "").upper() == api_symbol:
                rate = m.get("fundingRate")
                if rate is None:
                    raise Exception(f"No funding rate available for {symbol}")
                base_symbol = m.get("baseSymbol", "").upper()
                unified_symbol = f"{base_symbol}USDT" if base_symbol else api_symbol.replace("_", "")
                return {
                    "exchange": self.name,
                    "symbol": unified_symbol,
                    "rate": float(rate),
                    "timestamp": ts_now,
                    "interval_hours": interval_hours,
                    "fundingInterval": interval_ms,
                }
        raise Exception(f"Symbol {symbol} not found on Backpack")

    async def get_all_funding_rates(self) -> list[dict]:
        markets = await self._get_markets()
        ts_now = int(time.time() * 1000)
        perps = [m for m in markets if m.get("marketType") == "PERP"]
        results = []
        failures = 0

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            semaphore = asyncio.Semaphore(10)

            async def fetch_one(m):
                nonlocal failures
                interval_ms = m.get("fundingInterval")
                symbol_api = m.get("symbol", "")
                base = m.get("baseSymbol", "").upper()
                if not symbol_api or not base:
                    failures += 1
                    return None
                unified_symbol = f"{base}USDT"
                interval_hours = float(interval_ms) / 3600_000 if interval_ms else None
                rate = None
                ts = ts_now
                url = f"{self.base_url}/api/v1/fundingRates"
                params = {"symbol": symbol_api, "limit": 1}
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if isinstance(data, list) and data:
                                rate = data[0].get("fundingRate")
                                ts = self._parse_ts(data[0].get("intervalEndTimestamp"), ts_now)
                            else:
                                self.logger.warning("History empty for %s", symbol_api)
                        else:
                            failures += 1
                            self.logger.warning("HTTP %s for %s", resp.status, symbol_api)
                            return None
                except Exception as e:
                    failures += 1
                    self.logger.warning("Exception for %s: %s", symbol_api, e)
                    return None

                if rate is None:
                    failures += 1
                    return None

                return {
                    "exchange": self.name,
                    "symbol": unified_symbol,
                    "rate": float(rate),
                    "timestamp": ts,
                    "fundingInterval": interval_ms,
                    "interval_hours": interval_hours,
                }

            async def guarded_fetch(m):
                async with semaphore:
                    return await fetch_one(m)

            fetched = await asyncio.gather(*(guarded_fetch(m) for m in perps))
            results = [r for r in fetched if r is not None]

        # self.logger.info("Perps=%d success=%d failures=%d", len(perps), len(results), failures)
        return results
