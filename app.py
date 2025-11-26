import asyncio
import time
import json
import os
from pathlib import Path
from uuid import uuid4
import logging

import pandas as pd
import streamlit as st

from exchanges.aster import Aster
from exchanges.edgex import EdgeX
from exchanges.lighter import Lighter
from exchanges.hyperliquid import Hyperliquid
from exchanges.binance import Binance
from exchanges.backpack import Backpack

# Optional auto-refresh
try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger("funding_monitor")

st.set_page_config(page_title="Funding Fee Monitor", layout="wide")
st.title("Crypto Funding Fee Monitor")
if st_autorefresh:
    st_autorefresh(interval=60_000, key="data_refresh")

SOCIAL_HTML = """
<style>
.social-container {
  position: absolute;
  top: 10px;
  right: 16px;
  z-index: 100;
}
.social-row {display:flex; gap:10px; margin:0;}
.social-row a {
  display:inline-flex;
  align-items:center;
  gap:6px;
  padding:6px 10px;
  border-radius:10px;
  text-decoration:none;
  color:#fff;
  font-weight:600;
  font-size:14px;
  box-shadow:0 2px 6px rgba(0,0,0,0.15);
}
.social-row a:hover {opacity:0.92;}
.social-row .x-link {background:#111;}
.social-row .tg-link {background:#229ED9;}
</style>
<div class="social-container">
  <div class="social-row">
    <a class="x-link" href="https://x.com/0xYuCry" target="_blank" rel="noopener noreferrer">✕ <span>X</span></a>
    <a class="tg-link" href="https://t.me/journey_of_someone" target="_blank" rel="noopener noreferrer">✈ <span>Telegram</span></a>
  </div>
</div>
"""
st.markdown(SOCIAL_HTML, unsafe_allow_html=True)

VISIT_LOG_PATH = Path("visit_log.jsonl")

def record_visit_once():
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = str(uuid4())
    if st.session_state.get("visit_recorded"):
        return

    ts_ms = int(time.time() * 1000)
    entry = {
        "ts": ts_ms,
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(ts_ms / 1000)),
        "session": st.session_state["session_id"],
    }
    try:
        with VISIT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        # Keep UI clean; log server-side only
        logger.error("Visit log write failed: %s", e)
    st.session_state["visit_recorded"] = True

def get_visit_count() -> int | None:
    try:
        with VISIT_LOG_PATH.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0
    except Exception as e:
        logger.error("Visit log read failed: %s", e)
        return None

record_visit_once()

# Defaults if interval is missing
DEFAULT_INTERVAL_HOURS = {
    "Aster": 8,
    "EdgeX": 4,
    "Lighter": 1,
    "Hyperliquid": 1,
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
                # fall through to rounding below
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
        "Hyperliquid": 1,
        "Backpack": 1,
        "EdgeX": 4,
    }
    if exchange_name in fixed:
        return fixed[exchange_name]

    # Defaults for others (Aster/Binance use inferred payload when available)
    return DEFAULT_INTERVAL_HOURS.get(exchange_name, 8)

def calculate_apy(rate, interval_hours=8):
    if rate is None:
        return None
    return rate * (24 / interval_hours) * 365 * 100

@st.cache_data(ttl=60)
def get_all_rates():
    exchanges = [Aster(), EdgeX(), Lighter(), Hyperliquid(), Binance(), Backpack()]

    async def fetch_one(exchange):
        start = time.time()
        try:
            rates = await exchange.get_all_funding_rates()
            duration = time.time() - start
            logger.info("[Fetch] %s success: %d items in %.2fs", exchange.name, len(rates), duration)
            return {"exchange_name": exchange.name, "rates": rates, "duration": duration}
        except Exception as e:
            duration = time.time() - start
            logger.error("[Fetch] %s failed in %.2fs: %s", exchange.name, duration, e)
            return {"exchange_name": exchange.name, "rates": e, "duration": duration}

    async def fetch_all():
        tasks = [fetch_one(ex) for ex in exchanges]
        return await asyncio.gather(*tasks)

    return asyncio.run(fetch_all())

with st.status("Fetching funding rates...", expanded=True) as status_box:
    raw_results_with_names = get_all_rates()
    for entry in raw_results_with_names:
        ex = entry["exchange_name"]
        dur = entry.get("duration", 0)
        rates = entry["rates"]
        if isinstance(rates, Exception):
            status_box.write(f"{ex}: failed in {dur:.2f}s -> {rates}")
        else:
            status_box.write(f"{ex}: {len(rates)} items in {dur:.2f}s")
    status_box.update(label=f"Fetch complete @ {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}", state="complete", expanded=True)

# Build data_map with rate + interval
data_map = {}
skip_logs = []

for res_entry in raw_results_with_names:
    exchange_name = res_entry["exchange_name"]
    res = res_entry["rates"]
    if isinstance(res, Exception):
        continue
    for item in res:
        symbol = normalize_symbol(item["symbol"])
        interval_hours = extract_interval_hours(item, exchange_name)
        rate = item.get("rate")
        if symbol not in data_map:
            data_map[symbol] = {}
        data_map[symbol][exchange_name] = {"rate": rate, "interval": interval_hours}

if not data_map:
    st.error("No data collected after fetch. Check logs above for per-exchange errors.")

rows = []
for symbol, rates in data_map.items():
    row = {"Symbol": symbol}

    def set_rate(ex):
        info = rates.get(ex)
        if not info:
            return None
        rate = info["rate"]
        interval = info["interval"]
        row[f"{ex} Rate"] = rate
        row[f"{ex} Interval (h)"] = interval
        row[f"{ex} APY%"] = calculate_apy(rate, interval)
        return row[f"{ex} APY%"]

    apys = []
    for ex in ["Aster", "EdgeX", "Lighter", "Hyperliquid", "Binance", "Backpack"]:
        val = set_rate(ex)
        if val is not None:
            apys.append(val)

    if len([a for a in apys if a is not None]) < 2:
        skip_logs.append(symbol)
        continue

    valid_apys = [a for a in apys if a is not None]
    row["APY Spread (%)"] = max(valid_apys) - min(valid_apys)
    rows.append(row)

if skip_logs:
    st.caption(f"Skipped {len(skip_logs)} symbols with <2 exchanges. Sample: {skip_logs[:5]}")

df = pd.DataFrame(rows)
# Reorder columns to surface spread near symbol
if "APY Spread (%)" in df.columns:
    cols = df.columns.tolist()
    new_cols = ["Symbol", "APY Spread (%)"] + [c for c in cols if c not in ("Symbol", "APY Spread (%)")]
    df = df[new_cols]

st.markdown(
    "**说明**：资金费率按小时拆分，用对应交易所/符号的结算周期计算年化：`rate × (24/周期) × 365 × 100`；"
    " SPREAD 基于年化 APY：`(最高APY - 最低APY)`。"
)

display_exchanges = ["Aster", "EdgeX", "Lighter", "Hyperliquid", "Binance", "Backpack"]
for ex in display_exchanges:
    rate_col = f"{ex} Rate"
    int_col = f"{ex} Interval (h)"
    if rate_col in df.columns and int_col in df.columns:
        df[rate_col] = df.apply(
            lambda r: "-" if pd.isna(r[rate_col]) else f"{r[rate_col]*100:.4f}% ({r[int_col]:.1f}h)",
            axis=1
        )
        df.drop(columns=[int_col], inplace=True)

styler = df.style.format({
    "Aster APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
    "EdgeX APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
    "Lighter APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
    "Hyperliquid APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
    "Binance APY%": lambda x: "{:.2f}%".format(x) if x is not None else "-",
    **({"Backpack APY%": (lambda x: "{:.2f}%".format(x) if x is not None else "-")} if "Backpack APY%" in df.columns else {}),
    "APY Spread (%)": lambda x: "{:.2f}%".format(x) if x is not None else "-",
})
spread_cols = [c for c in ["Aster APY%", "EdgeX APY%", "Lighter APY%", "Hyperliquid APY%", "Binance APY%", "Backpack APY%"] if c in df.columns]
if spread_cols:
    styler = styler.background_gradient(subset=spread_cols, cmap="RdYlGn", vmin=-50, vmax=50)
if "APY Spread (%)" in df.columns:
    # cap extremes to avoid a single outlier skewing colors
    spread_vmin, spread_vmax = 0, 100
    try:
        if not df["APY Spread (%)"].empty:
            spread_vmin = max(0, df["APY Spread (%)"].quantile(0.05))
            spread_vmax = df["APY Spread (%)"].quantile(0.95)
            if spread_vmax <= spread_vmin:
                spread_vmax = spread_vmin + 1
    except Exception:
        pass
    styler = styler.background_gradient(subset=["APY Spread (%)"], cmap="Oranges", vmin=spread_vmin, vmax=spread_vmax)

st.dataframe(styler, width="stretch", height=800, hide_index=True)
