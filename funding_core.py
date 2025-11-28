import asyncio
import time
import logging
from typing import Iterable, Optional

from exchanges.aster import Aster
from exchanges.edgex import EdgeX
from exchanges.lighter import Lighter
from exchanges.hyperliquid import Hyperliquid
from exchanges.binance import Binance
from exchanges.backpack import Backpack

logger = logging.getLogger("funding_core")

DEFAULT_INTERVAL_HOURS = {
    "Aster": 8,
    "EdgeX": 4,
    "Lighter": 1,
    "HL": 1,
    "Binance": 8,
    "Backpack": 1,
}


def normalize_symbol(symbol: str) -> str:
    s = symbol.upper().replace("-", "").replace("_", "").replace("PERP", "")
    if s.endswith("USDC"):
        s = s[:-4] + "USDT"
    elif s.endswith("USD") and not s.endswith("USDT"):
        s = s + "T"
    if "TRUMP2" in s:
        s = s.replace("TRUMP2", "TRUMP")
    return s


def extract_interval_hours(item: dict, exchange_name: str) -> float:
    interval_hours = None
    # Priority: payload interval_hours -> fundingInterval diff -> nextFundingTime diff
    if item.get("interval_hours") is not None:
        try:
            return float(item["interval_hours"])
        except Exception:
            pass

    if item.get("fundingInterval"):
        try:
            return float(item["fundingInterval"]) / 3600_000
        except Exception:
            pass

    if item.get("nextFundingTime") and item.get("timestamp"):
        try:
            diff = int(item["nextFundingTime"]) - int(item["timestamp"])
            if diff > 0:
                interval_hours = diff / 3600_000
        except Exception:
            interval_hours = None

    # If we have an interval from above, snap close-to-integer values (e.g. 7.999999 -> 8)
    if interval_hours is not None:
        try:
            snapped = round(interval_hours)
            if abs(interval_hours - snapped) < 0.25:
                interval_hours = float(snapped)
        except Exception:
            pass
        return interval_hours

    # Fixed intervals
    fixed = {
        "Lighter": 1,
        "HL": 1,
        "Backpack": 1,
        "EdgeX": 4,
    }
    if exchange_name in fixed:
        return fixed[exchange_name]

    # Defaults for others (Aster/Binance use inferred payload when available)
    return DEFAULT_INTERVAL_HOURS.get(exchange_name, 8)


def calculate_apy(rate, interval_hours: float = 8):
    if rate is None or interval_hours in (None, 0):
        return None
    return rate * (24 / interval_hours) * 365 * 100


async def fetch_all_raw():
    exchanges = [(Aster(), "Aster"), (EdgeX(), "EdgeX"), (Lighter(), "Lighter"), (Hyperliquid(), "HL"), (Binance(), "Binance"), (Backpack(), "Backpack")]

    async def fetch_one(exchange_tuple):
        exchange, display_name = exchange_tuple
        start = time.time()
        try:
            rates = await exchange.get_all_funding_rates()
            duration = time.time() - start
            logger.info("[Fetch] %s success: %d items in %.2fs", display_name, len(rates), duration)
            return {"exchange_name": display_name, "rates": rates, "duration": duration}
        except Exception as e:
            duration = time.time() - start
            logger.error("[Fetch] %s failed in %.2fs: %s", display_name, duration, e)
            return {"exchange_name": display_name, "rates": None, "error": str(e), "duration": duration}

    return await asyncio.gather(*[fetch_one(ex) for ex in exchanges])


def generate_mock_data(rows=200):
    import random
    exchanges = ["Aster", "EdgeX", "Lighter", "HL", "Binance", "Backpack"]
    mock_results = []
    
    # Generate a list of symbols
    symbols = [f"BTC-USDT-{i}" for i in range(rows)]
    
    for ex_name in exchanges:
        rates = []
        for sym in symbols:
            # 10% chance of None
            if random.random() < 0.1:
                continue
                
            # Random rate between -0.05% and 0.05%
            rate = (random.random() - 0.5) * 0.001 
            
            # Random interval
            interval = random.choice([1, 4, 8])
            
            rates.append({
                "symbol": sym,
                "rate": rate,
                "interval_hours": interval,
                "timestamp": int(time.time() * 1000)
            })
            
        mock_results.append({
            "exchange_name": ex_name,
            "rates": rates,
            "duration": 0.1
        })
        
    return mock_results


def process_raw_results(raw_results, selected_exchanges: Optional[Iterable[str]] = None):
    if selected_exchanges is None:
        selected_exchanges = ["Aster", "EdgeX", "Lighter", "Hyperliquid", "Binance", "Backpack"]
    
    data_map: dict[str, dict] = {}

    for res_entry in raw_results:
        exchange_name = res_entry["exchange_name"]
        res = res_entry["rates"]
        if not res or isinstance(res, Exception): # Handle exceptions gracefully
            continue
        for item in res:
            symbol = normalize_symbol(item["symbol"])
            interval_hours = extract_interval_hours(item, exchange_name)
            rate = item.get("rate")
            data_map.setdefault(symbol, {})[exchange_name] = {
                "rate": rate,
                "interval": interval_hours,
            }

    rows = []
    for symbol, rates in data_map.items():
        row = {"Symbol": symbol}
        count = 0
        min_apy = None
        max_apy = None
        
        # We need to collect APYs to calculate spread
        apys = []

        for ex in selected_exchanges:
            info = rates.get(ex)
            rate_col = f"{ex} Rate"
            # int_col = f"{ex} Interval (h)" # We might not need this if we format directly, but app.py used it.
            # Let's stick to returning raw data and letting UI format it? 
            # Or return formatted strings? 
            # The original app.py calculated APY and Spread here.
            
            if not info:
                # row[rate_col] = None
                continue
            
            rate = info["rate"]
            interval = info["interval"]
            apy = calculate_apy(rate, interval)
            
            row[rate_col] = rate
            row[f"{ex} Interval (h)"] = interval
            row[f"{ex} APY%"] = apy
            
            if apy is not None:
                apys.append(apy)

        if len(apys) < 2:
            continue
            
        row["APY Spread (%)"] = max(apys) - min(apys)
        rows.append(row)

    return rows


async def build_table_rows(selected_exchanges: Optional[Iterable[str]] = None):
    """
    Returns (rows, raw_results) where rows are ready for tabular display.
    Each row contains Symbol, per-exchange rate/apy, and apy_spread.
    """
    raw_results = await fetch_all_raw()
    rows = process_raw_results(raw_results, selected_exchanges)
    return rows, raw_results
