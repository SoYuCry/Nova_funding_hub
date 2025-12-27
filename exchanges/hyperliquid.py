import aiohttp
import time
from .base import Exchange


class Hyperliquid(Exchange):
    def __init__(self):
        super().__init__("Hyperliquid", "https://api.hyperliquid.xyz")
        self.timeout = aiohttp.ClientTimeout(total=10)

    def _symbol_to_coin(self, symbol: str) -> str:
        s = symbol.upper()
        # Support xyz:* (equity markets) shown as app.hyperliquid.xyz/trade/xyz:NVDA
        if ":" in s:
            s = s.split(":", 1)[1]
        if s.endswith("USDT"):
            return s[:-4]
        if s.endswith("USD"):
            return s[:-3]
        return s

    def _coin_to_symbol(self, coin: str) -> str:
        coin = coin.upper()
        # Strip known namespace prefixes (e.g. xyz:NVDA -> NVDAUSDT) to align across exchanges
        if ":" in coin:
            coin = coin.split(":", 1)[1]
        if coin.endswith("USDT") or coin.endswith("USD"):
            return coin
        return coin + "USDT"

    async def _fetch_meta_and_ctx(self, dex: str | None = None):
        url = f"{self.base_url}/info"
        payload = {"type": "metaAndAssetCtxs"}
        if dex is not None:
            payload["dex"] = dex
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    raise Exception(f"Hyperliquid API error: {resp.status}")
                data = await resp.json()
                if not isinstance(data, list) or len(data) < 2:
                    raise Exception("Unexpected Hyperliquid response structure")
                meta, ctxs = data[0], data[1]
                universe = meta.get("universe") or []
                return universe, ctxs

    async def get_funding_rate(self, symbol: str) -> dict:
        # Try both default perps universe and trade[XYZ] universe (dex=xyz)
        coin = self._symbol_to_coin(symbol)
        for dex in (None, "xyz"):
            universe, ctxs = await self._fetch_meta_and_ctx(dex=dex)
            for meta, ctx in zip(universe, ctxs):
                name = (meta.get("name", "") or "").upper()
                if not name:
                    continue
                if ":" in name:
                    name = name.split(":", 1)[1]
                if name == coin:
                    rate = float(ctx.get("funding", 0.0))
                    ts = int(time.time() * 1000)
                    return {
                        "exchange": self.name,
                        "symbol": self._coin_to_symbol(meta.get("name", "")),
                        "rate": rate,
                        "timestamp": ts,
                        "interval_hours": 1,
                    }
        raise Exception(f"Symbol {symbol} not found on Hyperliquid (including dex=xyz)")

    async def get_all_funding_rates(self) -> list[dict]:
        results: list[dict] = []
        now_ms = int(time.time() * 1000)

        seen: set[str] = set()

        for dex in (None, "xyz"):
            universe, ctxs = await self._fetch_meta_and_ctx(dex=dex)
            for meta, ctx in zip(universe, ctxs):
                name = meta.get("name", "")
                if not name:
                    continue
                # Skip delisted symbols
                if meta.get("isDelisted", False):
                    continue
                symbol = self._coin_to_symbol(name)
                if symbol in seen:
                    continue
                seen.add(symbol)
                rate = float(ctx.get("funding", 0.0))
                results.append(
                    {
                        "exchange": self.name,
                        "symbol": symbol,
                        "rate": rate,
                        "timestamp": now_ms,
                        "interval_hours": 1,
                    }
                )
        return results
