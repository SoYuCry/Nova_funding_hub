import aiohttp
from .base import Exchange
import asyncio



import json
import os

CACHE_FILE = "binance_intervals.json"


class Binance(Exchange):
    def __init__(self):
        super().__init__("Binance", "https://fapi.binance.com")
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.interval_cache = self._load_cache()
        self._cache_dirty = False

    def _load_cache(self):
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r") as f:
                    data = json.load(f)
                return {k.upper(): v for k, v in data.items()}
        except Exception:
            pass
        return {}

    def _save_cache(self):
        if not self._cache_dirty:
            return
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(self.interval_cache, f, indent=2, sort_keys=True)
        except Exception:
            pass

    async def _fetch_interval_hours(self, symbol: str, session: aiohttp.ClientSession, nextFundingTime: int | None = None) -> float | None:
        if symbol in self.interval_cache:
            return self.interval_cache[symbol]
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {"symbol": symbol.upper(), "limit": 2}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()

                if isinstance(data, list) and len(data) >= 2:
                    if nextFundingTime is not None:
                        hrs = abs(nextFundingTime - t1) / 3600_000
                        if 7.9999 < hrs < 8.001:
                            hrs = 8
                        if 3.9999 < hrs < 4.001:
                            hrs = 4
                        if 0.9999 < hrs < 1.001:
                            hrs = 1
                        self.interval_cache[symbol.upper()] = hrs
                        self._cache_dirty = True
                        return hrs
                    # Fallback to calculating from the last two funding rates
                    else:
                        t1 = data[0].get("fundingTime")
                        t2 = data[1].get("fundingTime")
                        if isinstance(t1, int) and isinstance(t2, int):
                            hrs = abs(t1 - t2) / 3600_000
                            if 7.9999 < hrs < 8.001:
                                hrs = 8
                            if 3.9999 < hrs < 4.001:
                                hrs = 4
                            if 0.9999 < hrs < 1.001:
                                hrs = 1
                            self.interval_cache[symbol.upper()] = hrs
                            self._cache_dirty = True
                            return hrs
        except Exception:
            return None
        return None

    async def get_funding_rate(self, symbol: str) -> dict:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": symbol.upper()}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")
                data = await resp.json()
                nextFundingTime = int(data.get("nextFundingTime", 0)) if data.get("nextFundingTime") else None
                interval_hours = await self._fetch_interval_hours(symbol, session, nextFundingTime)

                return {
                    "exchange": self.name,
                    "symbol": symbol.upper(),
                    "rate": float(data["lastFundingRate"]),
                    "timestamp": int(data["time"]),
                    "nextFundingTime": nextFundingTime,
                    "interval_hours": interval_hours
                }

    async def get_all_funding_rates(self) -> list[dict]:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")
                data = await resp.json()
                results = []

                semaphore = asyncio.Semaphore(5)

                async def enrich(item):
                    async with semaphore:
                        hrs = await self._fetch_interval_hours(item["symbol"], session)
                        return {
                            "exchange": self.name,
                            "symbol": item["symbol"],
                            "rate": float(item["lastFundingRate"]),
                            "timestamp": int(item["time"]),
                            "nextFundingTime": int(item.get("nextFundingTime", 0)) if item.get("nextFundingTime") else None,
                            "interval_hours": hrs,
                        }

                results = await asyncio.gather(*(enrich(item) for item in data))
                self._save_cache()
                return results
