import aiohttp
import time
import asyncio
import json
import os
from .base import Exchange

class Aster(Exchange):
    def __init__(self):
        super().__init__("Aster", "https://fapi.asterdex.com")
        self.cache_file = "aster_intervals.json"
        self.interval_cache = self._load_cache()
        self._cache_dirty = False

    def _load_cache(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "r") as f:
                    data = json.load(f)
                return {k.upper(): round(float(v)) for k, v in data.items()}
        except Exception:
            pass
        return {}

    def _save_cache(self):
        if not self._cache_dirty:
            return
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self.interval_cache, f, indent=2, sort_keys=True)
        except Exception:
            pass

    async def _fetch_interval_hours(self, symbol: str, session: aiohttp.ClientSession, nextFundingTime: int = None) -> float | None:
        if symbol in self.interval_cache:
            return self.interval_cache[symbol]
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": 2}
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
        params = {"symbol": symbol}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"Aster API error: {response.status}")
                data = await response.json()
                nextFundingTime = int(data.get("nextFundingTime", 0)) if data.get("nextFundingTime") else None
                # If symbol is provided, data is a dict. If not, it's a list.
                # We assume symbol is provided.
                if isinstance(data, list):
                     # Handle case where list is returned (shouldn't happen with symbol param but good to be safe)
                     for item in data:
                         if item['symbol'] == symbol:
                             data = item
                             break
                
                # Use history to infer interval for single symbol; defaults handled upstream if None
                interval_hours = await self._fetch_interval_hours(symbol, session, nextFundingTime)

                return {
                    "exchange": self.name,
                    "symbol": symbol,
                    "rate": float(data["lastFundingRate"]),
                    "timestamp": int(data["time"]),
                    "nextFundingTime": nextFundingTime,
                    "interval_hours": interval_hours
                }

    async def get_all_funding_rates(self) -> list[dict]:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Aster API error: {response.status}")
                data = await response.json()

                semaphore = asyncio.Semaphore(5)

                async def enrich(item):
                    async with semaphore:
                        interval_hours = await self._fetch_interval_hours(item["symbol"], session)
                        return {
                            "exchange": self.name,
                            "symbol": item["symbol"],
                            "rate": float(item["lastFundingRate"]),
                            "timestamp": int(item["time"]),
                            "nextFundingTime": int(item.get("nextFundingTime", 0)) if item.get("nextFundingTime") else None,
                            "interval_hours": interval_hours,
                        }

                results = await asyncio.gather(*(enrich(item) for item in data))
                self._save_cache()
                return results
                return results
