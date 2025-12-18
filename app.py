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


if st_autorefresh:
    st_autorefresh(interval=60_000, key="data_refresh")

# 顶部右下角社交链接
ui_components.render_social_links()

# ============ 数据获取 ============

import threading

USE_MOCK_DATA = False  # True=本地假数据，False=真实拉取


@st.cache_resource
def start_background_fetcher(use_mock: bool = False):
    """
    后台线程：每隔 60s 拉一次数据，写到 data_store 里。
    UI 只读 data_store，不主动请求交易所。
    """
    data_store = {"data": None, "ts": None}
    lock = threading.Lock()

    def loop():
        while True:
            try:
                if use_mock:
                    raw = funding_core.generate_mock_data()
                else:
                    async def fetch():
                        return await funding_core.fetch_all_raw()
                    raw = asyncio.run(fetch())

                ts = time.time()
                with lock:
                    data_store["data"] = raw
                    data_store["ts"] = ts

                logger.info("Background fetch ok, ts=%s", ts)
            except Exception as e:
                logger.exception("Background fetch failed: %s", e)

            # 间隔 60 秒再拉
            time.sleep(60)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return data_store, lock


# 启动后台 fetcher（全局只启动一次）
data_store, data_lock = start_background_fetcher(USE_MOCK_DATA)

# 在当前这次渲染中读出一个快照
with data_lock:
    raw_results = data_store["data"]
    last_update_ts = data_store["ts"]

if raw_results is None:
    # 首次还没拉到数据
    st.markdown(
        '<div style="font-size:14px; opacity:0.85;">正在拉取数据，请稍后刷新...</div>',
        unsafe_allow_html=True,
    )
    st.stop()

last_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_update_ts))

# ============ 设置 ============
default_exchanges = funding_core.EXCHANGE_NAMES

# 标题 + 右上角齿轮在同一行
# title_col, gear_col = st.columns([8, 1])

# with title_col:
#     st.markdown(
#         '<div class="page-title">DEXs 资金费率面板</div>',
#         unsafe_allow_html=True,
#     )

# with gear_col:
#     selected_exchanges = ui_components.render_settings_popover(default_exchanges)

# 去掉齿轮版本
st.markdown(
    '<div class="page-title">DEXs 资金费率面板</div>',
    unsafe_allow_html=True,
)

# 固定样式（不再支持主题切换）
ui_components.render_global_theme_styles()

# 不用齿轮了，直接全选所有交易所
selected_exchanges = default_exchanges

# ============ 数据处理 ============

rows = funding_core.process_raw_results(raw_results, selected_exchanges)

if not rows:
    st.error("No data collected after fetch. Check logs above for per-exchange errors.")
else:
    df = pd.DataFrame(rows)

    # Reorder columns to surface spread near symbol
    if "Max Spread (%)" in df.columns:
        cols = df.columns.tolist()
        new_cols = ["Symbol", "Max Spread (%)"] + [
            c for c in cols if c not in ("Symbol", "Max Spread (%)")
        ]
        df = df[new_cols]

    ui_components.render_last_update(last_update)
    ui_components.render_rate_explanation()


    # Merge Rate + Interval
    for ex in default_exchanges:
        rate_col = f"{ex} Rate"
        int_col = f"{ex} Interval (h)"
        if rate_col in df.columns and int_col in df.columns:
            df[rate_col] = df.apply(
                lambda r: "-"
                if pd.isna(r[rate_col])
                else f"{r[rate_col]*100:.4f}% ({r[int_col]:.1f}h)",
                axis=1,
            )
            df.drop(columns=[int_col], inplace=True)

    # Render Table（带 sticky header + 主题）
    ui_components.render_rates_table(df)

# ============ 页面底部版权 + 访问量 ============

ui_components.render_visit_counter()
