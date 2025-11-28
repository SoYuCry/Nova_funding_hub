import asyncio
from exchanges.binance import Binance

# Simple live check against Binance funding API to verify interval parsing.
async def _fetch(symbol: str) -> dict:
    ex = Binance()
    return await ex.get_funding_rate(symbol)


def fetch_interval(symbol: str) -> tuple[float | None, dict]:
    data = asyncio.run(_fetch(symbol))
    return data.get("interval_hours"), data


def test_binance_interval_tnsr():
    interval_hours, data = fetch_interval("TNSRUSDT")
    assert interval_hours is not None and 0 < interval_hours < 12, f"Unexpected interval: {interval_hours}"
    assert data["symbol"] == "TNSRUSDT"
    assert isinstance(data["rate"], float)


if __name__ == "__main__":
    interval, data = fetch_interval("USDT")
    print(f"TNSR interval_hours={interval} rate={data['rate']} timestamp={data['timestamp']}")
