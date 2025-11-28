import asyncio
import time
import logging

import pandas as pd
import streamlit as st

import funding_core
import ui_components

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
st.title("DEXs 资金费率面板")

if st_autorefresh:
    st_autorefresh(interval=60_000, key="data_refresh")

# Render UI components
ui_components.render_social_links()
ui_components.render_visit_counter()

# Data Fetching
USE_MOCK_DATA = True

@st.cache_data(ttl=60)
def get_all_rates_cached(use_mock_data=False):
    if use_mock_data:
        return {"data": funding_core.generate_mock_data(), "ts": time.time()}
        
    # We use asyncio.run here because streamlit is sync by default (unless using async mode which is experimental/complex)
    # funding_core.fetch_all_raw is async
    async def fetch():
        return await funding_core.fetch_all_raw()
    
    raw_results = asyncio.run(fetch())
    return {"data": raw_results, "ts": time.time()}

res_bundle = get_all_rates_cached(USE_MOCK_DATA)
raw_results = res_bundle["data"]
last_update_ts = res_bundle["ts"]
last_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_update_ts))

# Status and Settings
default_exchanges = ["Aster", "EdgeX", "Lighter", "HL", "Binance", "Backpack"]

status_row = st.columns([5, 1])
with status_row[0]:
    st.caption(f"Last update: {last_update}")
with status_row[1]:
    # Pass default_exchanges to the popover
    selected_exchanges = ui_components.render_settings_popover(default_exchanges)

# Process Data
rows = funding_core.process_raw_results(raw_results, selected_exchanges)

if not rows:
    st.error("No data collected after fetch. Check logs above for per-exchange errors.")
else:
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

    # Merge Rate + Interval
    for ex in default_exchanges:
        rate_col = f"{ex} Rate"
        int_col = f"{ex} Interval (h)"
        if rate_col in df.columns and int_col in df.columns:
            df[rate_col] = df.apply(
                lambda r: "-" if pd.isna(r[rate_col]) else f"{r[rate_col]*100:.4f}% ({r[int_col]:.1f}h)",
                axis=1
            )
            df.drop(columns=[int_col], inplace=True)

    # Render Table
    ui_components.render_rates_table(df)
