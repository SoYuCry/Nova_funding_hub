import time
import json
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
import textwrap
import streamlit as st

logger = logging.getLogger("funding_monitor")

PALETTE = {
    "wrapper_bg": "var(--background-color)",
    "table_bg": "var(--background-color)",
    "text": "var(--text-color)",
    "border": "rgba(128, 128, 128, 0.2)",
    "row_border": "rgba(128, 128, 128, 0.1)",
    "hover": "var(--secondary-background-color)",
    "head_shadow": "var(--background-color)",
}

SOCIAL_HTML = """
<style>
.social-container {
  position: fixed;
  bottom: 20px;
  right: 24px;
  z-index: 1000;
  opacity: 0.6;
  transition: opacity 0.3s ease;
}
.social-container:hover {
  opacity: 1;
}
.social-row {display:flex; gap:12px; margin:0;}
.social-row a {
  display:inline-flex;
  align-items:center;
  gap:8px;
  padding:8px 14px;
  border-radius:20px; /* More rounded */
  text-decoration:none;
  color:#fff;
  font-weight: 500;
  font-size:13px;
  backdrop-filter: blur(4px);
  box-shadow:0 4px 12px rgba(0,0,0,0.1);
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.social-row a:hover {
    transform: translateY(-2px);
    box-shadow:0 6px 16px rgba(0,0,0,0.15);
}
.social-row .x-link {background: rgba(0,0,0,0.6);}
.social-row .tg-link {background: rgba(34, 158, 217, 0.8);}
</style>
<div class="social-container">
  <div class="social-row">
    <a class="x-link" href="https://x.com/0xYuCry" target="_blank" rel="noopener noreferrer">
      <span>𝕏</span> @0xYuCry
    </a>
    <a class="tg-link" href="https://t.me/+gBbEJUXAKn81NGJl" target="_blank" rel="noopener noreferrer">
      <span>✈</span> Telegram
    </a>
  </div>
</div>
"""

VISIT_LOG_PATH = Path("visit_log.jsonl")


def render_social_links():
    st.markdown(SOCIAL_HTML, unsafe_allow_html=True)


def render_global_theme_styles():
    palette = PALETTE
    st.markdown(
        f"""
        <style>
        /* overall spacing and base text */
        [data-testid="stAppViewContainer"] > div:nth-child(1) .block-container,
        [data-testid="stAppViewContainer"] .main .block-container {{
            padding-top: 0.5rem !important;
        }}
        /* normalize markdown/text colors so small helper texts stay visible */
        .stMarkdown, .stMarkdown p, .stMarkdown div, .markdown-text-container, label, .stRadio > label {{
            color: {palette["text"]} !important;
        }}
        .stMarkdown p {{
            margin: 0.1rem 0 0.35rem;
        }}

        /* 页面标题专用样式 */
        .page-title {{
            font-size: 2.2rem;
            font-weight: 800;
            line-height: 1.2;
            margin-bottom: 1rem;
            color: {palette["text"]};
            letter-spacing: -0.02em;
        }}

        h1, h2, h3, h4, h5, h6 {{
            color: {palette["text"]} !important;
            margin-bottom: 0.5rem;
            font-weight: 600;
        }}

        /* gear button */
        button[data-testid="stPopover"] {{
            background: transparent;
            color: {palette["text"]};
            border: 1px solid {palette["border"]};
            box-shadow: none;
        }}
        button[data-testid="stPopover"]:hover {{
            background: {palette["hover"]};
            border-color: {palette["text"]};
            color: {palette["text"]};
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def record_visit_once():
    if "session_id" not in st.session_state:
        st.session_state["session_id"] = str(uuid4())
    if st.session_state.get("visit_recorded"):
        return

    ts_ms = int(time.time() * 1000)
    entry = {
        "ts": ts_ms,
        "ts_iso": time.strftime(
            "%Y-%m-%dT%H:%M:%S%z", time.localtime(ts_ms / 1000)
        ),
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


def render_visit_counter():
    """底部版权 + 访问量"""
    record_visit_once()
    visit_count = get_visit_count()
    year = time.localtime().tm_year
    if visit_count is not None:
        st.markdown(
            f"""
            <div style="margin-top:2.5rem; text-align:center; font-size:12px; color:#9ca3af;">
              © {year} Nova-BTC. All rights reserved. 总访问量：{visit_count}
            </div>
            """,
            unsafe_allow_html=True,
        )
def render_settings_popover(default_exchanges):
    """
    设置齿轮：只包含 交易所选择（已移除主题切换）
    """
    # 固定用暗色调来渲染齿轮按钮外观
    palette = PALETTE

    st.markdown(
        f"""
    <style>
    /* 让 popover 容器在当前列里靠右，不要拉伸按钮 */
    div[data-testid="stPopover"] {{
        display: flex;
        justify-content: flex-end;
    }}

    /* 真正的按钮在这个里面，强制变成小圆按钮 */
    div[data-testid="stPopover"] > button {{
        width: 28px;
        height: 28px;
        padding: 0;
        min-width: 0;
        border-radius: 999px;
        display: inline-flex;
        align-items: center;
        justify-content: center;

        background: {palette["table_bg"]};
        color: {palette["text"]};
        border: 1px solid {palette["border"]};

        transition: all 0.15s ease;
        cursor: pointer;
    }}

    div[data-testid="stPopover"] > button:hover {{
        background: {palette["hover"]};
        transform: scale(1.05);
    }}
    </style>
    """,
        unsafe_allow_html=True,
    )

    # 初始化交易所多选
    if "selected_exchanges" not in st.session_state:
        st.session_state["selected_exchanges"] = list(default_exchanges)

    for ex in default_exchanges:
        key = f"chk_{ex}"
        if key not in st.session_state:
            st.session_state[key] = ex in st.session_state["selected_exchanges"]

    try:
        with st.popover("⚙"):
            st.markdown("**展示的交易所**")
            rows = [st.columns(3), st.columns(3)]
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    st.checkbox(ex, key=f"chk_{ex}")
    except Exception:
        with st.expander("⚙"):
            st.markdown("**展示的交易所**")
            rows = [st.columns(3), st.columns(3)]
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    st.checkbox(ex, key=f"chk_{ex}")

    current_selection = []
    for ex in default_exchanges:
        if st.session_state.get(f"chk_{ex}", False):
            current_selection.append(ex)

    st.session_state["selected_exchanges"] = current_selection
    return current_selection


def render_rate_explanation():
    palette = PALETTE
    st.markdown(
        f"""
        <div style="
            font-size:14px;
            color:{palette['text']};
            opacity:0.9;
            margin-top:0.35rem;
            margin-bottom:0.35rem;
        ">
          说明：资金费率按小时拆分，用对应交易所/符号的结算周期计算年化：
          <code>rate × (24 / 周期) × 365 × 100</code>；
          Max Spread 基于年化 APY：<code>(最高 APY - 最低 APY)</code>。
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_last_update(ts: str):
    palette = PALETTE
    st.markdown(
        f"""
        <div style="
            text-align:left;
            font-size:14px;
            color:{palette['text']};
            font-weight:500;
            opacity:0.85;
            margin-top:0.35rem;
            margin-bottom:0.35rem;
        ">
          Last update: {ts}
        </div>
        """,
        unsafe_allow_html=True,
    )




def _highlight_extremes(row, spread_cols):
    # 与 spread_cols 对齐
    apy_cols = [c for c in spread_cols if c in row.index]
    styles = ["" for _ in apy_cols]
    if not apy_cols:
        return styles
    vals = row[apy_cols].dropna()
    if vals.empty:
        return styles
    max_v = vals.max()
    min_v = vals.min()
    for idx, col in enumerate(apy_cols):
        if pd.isna(row[col]):
            continue
        if row[col] == max_v:
            styles[idx] = (
                "box-shadow: 0 0 0 1px rgba(0,200,120,0.45); border-radius: 6px;"
            )
        elif row[col] == min_v:
            styles[idx] = (
                "box-shadow: 0 0 0 1px rgba(255,120,180,0.45); border-radius: 6px;"
            )
    return styles


def render_rates_table(df):
    # Styling Logic
    palette = PALETTE
    fmt_dict = {}
    def _fmt_pct(x):
        return "-" if pd.isna(x) else "{:.2f}%".format(x)

    apy_cols_in_df = [
        c for c in df.columns if c.endswith("APY%") and c != "Max Spread APY (%)"
    ]
    for col in apy_cols_in_df:
        fmt_dict[col] = _fmt_pct

    if "Max Spread APY (%)" in df.columns:
        fmt_dict["Max Spread APY (%)"] = _fmt_pct

    styler = df.style.format(fmt_dict)

    # Gradient Coloring
    spread_cols = apy_cols_in_df.copy()
    if spread_cols:
        styler = styler.background_gradient(
            subset=spread_cols,
            cmap="RdYlGn",
            vmin=-50,
            vmax=50,
        )
    if "Max Spread APY (%)" in df.columns:
        spread_vmin, spread_vmax = 0, 100
        try:
            if not df["Max Spread APY (%)"].empty:
                spread_vmin = max(0, df["Max Spread APY (%)"].quantile(0.05))
                spread_vmax = df["Max Spread APY (%)"].quantile(0.95)
                if spread_vmax <= spread_vmin:
                    spread_vmax = spread_vmin + 1
        except Exception:
            pass
        styler = styler.background_gradient(
            subset=["Max Spread APY (%)"],
            cmap="Oranges",
            vmin=spread_vmin,
            vmax=spread_vmax,
        )

    if spread_cols:
        styler = styler.apply(
            _highlight_extremes, spread_cols=spread_cols, subset=spread_cols, axis=1
        )

    def _na_bg(series):
        return [
            f"background-color: {palette['table_bg']}; color: {palette['text']};"
            if pd.isna(v) else ""
            for v in series
        ]

    if spread_cols:
        styler = styler.apply(_na_bg, subset=spread_cols)
    if "Max Spread APY (%)" in df.columns:
        styler = styler.apply(_na_bg, subset=["Max Spread APY (%)"])

    # HTML Rendering
    html = styler.to_html()

    # Generate unique ID for this table instance
    import random

    table_id = f"sortable_table_{random.randint(1000, 9999)}"

    # Inject table ID into the HTML
    html_with_id = html.replace("<table", f'<table id="{table_id}"', 1)

    palette = PALETTE
    top_offset = "3rem"

    css_block = textwrap.dedent(
        f"""
        <style>
        /* 整个表格基础样式 */
        .custom-table-container table {{
            width: 100%;
            border-collapse: separate; /* Changed to separate for better sticky handling if needed, but collapse is fine usually. Let's stick to collapse but remove borders. */
            border-collapse: collapse;
            border-spacing: 0;
            background-color: {palette["table_bg"]};
            color: {palette["text"]};
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            font-size: 14px;
        }}

        .custom-table-container thead {{
            position: sticky;
            top: {top_offset};
            z-index: 998;
            background-color: {palette["table_bg"]};
        }}

        /* 表头单元格 */
        .custom-table-container thead th {{
            background-color: {palette["table_bg"]};
            z-index: 999;
            padding: 8px 8px; /* 调回紧凑内边距 */
            text-align: center;
            border-bottom: 1px solid {palette["border"]};
            /* border-right: 1px solid {palette["border"]};  去掉竖线 */
            font-weight: 600;
            cursor: pointer;
            user-select: none;
            color: {palette["text"]};
            white-space: nowrap;
            letter-spacing: 0.02em;
            font-size: 13px; /* 稍微减小字号 */
        }}

        /* 最后一列表头 */
        .custom-table-container thead th:last-child {{
            border-right: none;
        }}

        /* 默认的上下箭头提示 - 稍微淡一点 */
        .custom-table-container thead th::after {{
            content: ' ⇅';
            font-size: 0.7rem;
            opacity: 0.2;
            margin-left: 4px;
            color: {palette["text"]};
            transition: opacity 0.2s;
        }}
        .custom-table-container thead th:hover::after {{
            opacity: 0.6;
        }}

        /* 升序 ▲ */
        .custom-table-container thead th.sort-asc::after {{
            content: ' ▲';
            opacity: 1;
            color: #4caf50;
        }}

        /* 降序 ▼ */
        .custom-table-container thead th.sort-desc::after {{
            content: ' ▼';
            opacity: 1;
            color: #f44336;
        }}

        /* 表体单元格样式 */
        .custom-table-container tbody td {{
            padding: 8px 8px; /* 调回紧凑内边距 */
            text-align: center;
            border-bottom: 1px solid {palette["row_border"]};
            /* border-right: 1px solid {palette["row_border"]}; 去掉竖线 */
            color: {palette["text"]};
            white-space: nowrap;
            font-variant-numeric: tabular-nums;
            font-size: 13px; /* 稍微减小字号 */
        }}

        .custom-table-container tbody td:last-child {{
            border-right: none;
        }}

        .custom-table-container tbody tr {{
            transition: background-color 0.15s ease;
        }}

        .custom-table-container tbody tr:hover {{
            background-color: {palette["hover"]};
        }}
        </style>
        """
    )




    # Render style and then the table HTML separately to avoid Markdown treating it as code
    st.markdown(css_block, unsafe_allow_html=True)
    st.markdown(
        f'<div class="custom-table-wrapper"><div class="custom-table-container">{html_with_id}</div></div>',
        unsafe_allow_html=True,
    )

    # JS 排序脚本：用普通字符串 + replace 避免 f-string 与 {{}} 冲突
    sort_script = textwrap.dedent(
        """
        <script>
        (function() {
            function initTableSort(attempt) {
                const doc = window.parent.document;
                const table = doc.getElementById('__TABLE_ID__');
                if (!table) {
                    if (attempt < 8) {
                        setTimeout(() => initTableSort(attempt + 1), 150);
                    }
                    return;
                }
                const headers = table.querySelectorAll('thead th');
                if (!headers.length) return;
                let currentSort = { col: -1, asc: true };
                headers.forEach((header, index) => {
                    header.addEventListener('click', function() {
                        const tbody = table.querySelector('tbody');
                        const rows = Array.from(tbody.querySelectorAll('tr'));
                        const asc = currentSort.col === index ? !currentSort.asc : true;
                        currentSort = { col: index, asc };
                        headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
                        header.classList.add(asc ? 'sort-asc' : 'sort-desc');
                        rows.sort((a, b) => {
                            const aCell = a.cells[index];
                            const bCell = b.cells[index];
                            if (!aCell || !bCell) return 0;
                            let aVal = aCell.textContent.trim().replace('%', '').replace(',', '');
                            let bVal = bCell.textContent.trim().replace('%', '').replace(',', '');
                            const aNum = parseFloat(aVal);
                            const bNum = parseFloat(bVal);
                            if (!isNaN(aNum) && !isNaN(bNum)) {
                                return asc ? aNum - bNum : bNum - aNum;
                            }
                            return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                        });
                        rows.forEach(row => tbody.appendChild(row));
                    });
                });
            }
            initTableSort(0);
            setTimeout(() => initTableSort(0), 300);
        })();
        </script>
        """
    ).replace("__TABLE_ID__", table_id)

    st.components.v1.html(sort_script, height=0, width=0, scrolling=False)
