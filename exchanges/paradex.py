import aiohttp
import os
import time

from .base import Exchange


class Paradex(Exchange):
    """
    Paradex public market data adapter.

    Uses:
    - GET /v1/markets
    - GET /v1/markets/summary?market=ALL
    """

    def __init__(self):
        base_url = os.getenv("PARADEX_BASE_URL", "https://api.prod.paradex.trade")
        super().__init__("Paradex", base_url)
        self.timeout = aiohttp.ClientTimeout(total=15)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (compatible; NovaFundingHub/1.0)"
        }

    def _to_market_symbol(self, symbol: str) -> str:
        """
        Convert normalized symbol (e.g. BTCUSDT/BTCUSD) to Paradex market symbol.
        Paradex perps are typically formatted as {BASE}-USD-PERP.
        """
        s = symbol.upper().replace("-", "").replace("_", "")
        if s.endswith("USDT"):
            base = s[:-4]
        elif s.endswith("USD"):
            base = s[:-3]
        else:
            base = s
        return f"{base}-USD-PERP"

    def _market_symbol_to_symbol(self, market_symbol: str) -> str:
        """
        Convert Paradex market symbol to our canonical symbol (USDT-suffixed).
        Example: BTC-USD-PERP -> BTCUSDT
        """
        s = market_symbol.upper()
        if s.endswith("-USD-PERP"):
            base = s[: -len("-USD-PERP")]
            return f"{base}USDT"
        return s.replace("-", "")

    async def _fetch_markets(self, session: aiohttp.ClientSession) -> dict[str, float]:
        url = f"{self.base_url}/v1/markets"
        async with session.get(url, headers=self.headers) as resp:
            if resp.status != 200:
                body = (await resp.text())[:200]
                raise Exception(f"Paradex markets error: {resp.status} body={body}")
            data = await resp.json()

        result: dict[str, float] = {}
        for item in data.get("results", []):
            if item.get("asset_kind") != "PERP":
                continue
            market_symbol = item.get("symbol")
            if not market_symbol:
                continue
            try:
                interval = float(item.get("funding_period_hours") or 8)
            except Exception:
                interval = 8.0
            result[str(market_symbol)] = interval
        return result

    async def _fetch_summary(self, session: aiohttp.ClientSession, market: str) -> list[dict]:
        url = f"{self.base_url}/v1/markets/summary"
        params = {"market": market}
        async with session.get(url, params=params, headers=self.headers) as resp:
            if resp.status != 200:
                body = (await resp.text())[:200]
                raise Exception(f"Paradex summary error: {resp.status} body={body}")
            data = await resp.json()
        return data.get("results", [])

    async def get_funding_rate(self, symbol: str) -> dict:
        market_symbol = self._to_market_symbol(symbol)
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            intervals = await self._fetch_markets(session)
            interval_hours = intervals.get(market_symbol)
            summary = await self._fetch_summary(session, market_symbol)
            if not summary:
                raise Exception(f"Paradex market not found in summary: {market_symbol}")
            item = summary[0]
            rate = float(item.get("funding_rate", 0.0))
            ts = int(item.get("created_at") or time.time() * 1000)
            return {
                "exchange": self.name,
                "symbol": self._market_symbol_to_symbol(market_symbol),
                "rate": rate,
                "timestamp": ts,
                "interval_hours": interval_hours,
            }

    async def get_all_funding_rates(self) -> list[dict]:
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            intervals = await self._fetch_markets(session)
            summary = await self._fetch_summary(session, "ALL")

        now_ms = int(time.time() * 1000)
        results: list[dict] = []
        for item in summary:
            market_symbol = item.get("symbol")
            if not market_symbol:
                continue
            # Only include PERP markets we saw in /v1/markets
            interval_hours = intervals.get(market_symbol)
            if interval_hours is None:
                continue
            rate = item.get("funding_rate")
            if rate is None:
                continue
            try:
                rate_f = float(rate)
            except Exception:
                continue
            ts = int(item.get("created_at") or now_ms)
            results.append(
                {
                    "exchange": self.name,
                    "symbol": self._market_symbol_to_symbol(market_symbol),
                    "rate": rate_f,
                    "timestamp": ts,
                    "interval_hours": interval_hours,
                }
            )
        return results

