import time
import json
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
import streamlit as st

logger = logging.getLogger("funding_monitor")

SOCIAL_HTML = """
<style>
.social-container {
  position: fixed;
  bottom: 14px;
  right: 16px;
  z-index: 1000;
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

VISIT_LOG_PATH = Path("visit_log.jsonl")

def render_social_links():
    st.markdown(SOCIAL_HTML, unsafe_allow_html=True)

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

def render_visit_counter():
    record_visit_once()
    visit_count = get_visit_count()
    if visit_count is not None:
        st.caption(f"总访问量 {visit_count}")

def render_settings_popover(default_exchanges):
    # square-ish popover trigger for the gear
    st.markdown(
        """
    <style>
    button[data-testid="stPopover"] {
      width: 40px;
      height: 40px;
      padding: 6px;
      border-radius: 10px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )
    
    # Initialize session state for checkboxes if not present
    if "selected_exchanges" not in st.session_state:
        st.session_state["selected_exchanges"] = list(default_exchanges)
        
    # Ensure individual keys exist for binding
    for ex in default_exchanges:
        key = f"chk_{ex}"
        if key not in st.session_state:
            st.session_state[key] = ex in st.session_state["selected_exchanges"]

    try:
        with st.popover("⚙", width="stretch"):
            st.markdown("**展示的交易所**")
            rows = [st.columns(3), st.columns(3)]
            
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    # Bind directly to session state key
                    st.checkbox(ex, key=f"chk_{ex}")
            
    except Exception:
        # Fallback
        with st.expander("⚙"):
            rows = [st.columns(3), st.columns(3)]
            for idx, ex in enumerate(default_exchanges):
                row = rows[idx // 3]
                with row[idx % 3]:
                    st.checkbox(ex, key=f"chk_{ex}")

    # Reconstruct selected_exchanges from keys
    current_selection = []
    for ex in default_exchanges:
        if st.session_state.get(f"chk_{ex}", False):
            current_selection.append(ex)
            
    st.session_state["selected_exchanges"] = current_selection
    return current_selection

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
            styles[idx] = "box-shadow: 0 0 0 1px rgba(0,200,120,0.45); border-radius: 6px;"
        elif row[col] == min_v:
            styles[idx] = "box-shadow: 0 0 0 1px rgba(255,120,180,0.45); border-radius: 6px;"
    return styles


def render_rates_table(df):
    # Styling Logic
    fmt_dict = {}
    apy_cols_in_df = [c for c in df.columns if c.endswith("APY%") and c != "APY Spread (%)"]
    for col in apy_cols_in_df:
        fmt_dict[col] = (lambda x: "{:.2f}%".format(x) if x is not None else "-")

    if "APY Spread (%)" in df.columns:
        fmt_dict["APY Spread (%)"] = (lambda x: "{:.2f}%".format(x) if x is not None else "-")

    styler = df.style.format(fmt_dict)

    # Gradient Coloring
    spread_cols = apy_cols_in_df.copy()
    if spread_cols:
        styler = styler.background_gradient(subset=spread_cols, cmap="RdYlGn", vmin=-50, vmax=50)
    if "APY Spread (%)" in df.columns:
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

    if spread_cols:
        styler = styler.apply(_highlight_extremes, spread_cols=spread_cols, subset=spread_cols, axis=1)

    # HTML Rendering
    html = styler.to_html()
    
    # Generate unique ID for this table instance
    import random
    table_id = f"sortable_table_{random.randint(1000, 9999)}"
    
    st.markdown(
        f"""
        <style>
        /* Container styling */
        .custom-table-container {{
            width: 100%;
        }}
        
        /* Table styling */
        .custom-table-container table {{
            width: 100%;
            border-collapse: collapse;
            font-family: "Source Sans Pro", sans-serif;
            font-size: 14px;
        }}
        
        /* Sticky Header with solid background */
        .custom-table-container thead tr th {{
            position: sticky;
            top: 3.75rem;
            background-color: #0e1117; /* Solid dark background */
            z-index: 999;
            padding: 8px;
            text-align: right;
            border-bottom: 2px solid #444;
            box-shadow: 0 2px 4px rgba(0,0,0,0.3);
            cursor: pointer;
            user-select: none;
        }}
        
        /* Light mode header background */
        @media (prefers-color-scheme: light) {{
            .custom-table-container thead tr th {{
                background-color: #ffffff;
                border-bottom: 2px solid #ddd;
            }}
        }}
        
        /* Header hover effect */
        .custom-table-container thead tr th:hover {{
            background-color: #1a1d24;
        }}
        
        @media (prefers-color-scheme: light) {{
            .custom-table-container thead tr th:hover {{
                background-color: #f0f0f0;
            }}
        }}
        
        /* Sort indicator */
        .custom-table-container thead tr th::after {{
            content: ' ⇅';
            opacity: 0.3;
            font-size: 0.8em;
        }}
        
        .custom-table-container thead tr th.sort-asc::after {{
            content: ' ▲';
            opacity: 1;
        }}
        
        .custom-table-container thead tr th.sort-desc::after {{
            content: ' ▼';
            opacity: 1;
        }}
        
        /* Cell styling */
        .custom-table-container tbody tr td {{
            padding: 8px;
            text-align: right;
            border-bottom: 1px solid #333;
        }}
        
        /* Hover effect */
        .custom-table-container tbody tr:hover {{
            background-color: rgba(255, 255, 255, 0.05);
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    # Inject table ID into the HTML
    html_with_id = html.replace('<table', f'<table id="{table_id}"', 1)
    
    # Render table and JavaScript separately for better timing
    st.markdown(f'<div class="custom-table-container">{html_with_id}</div>', unsafe_allow_html=True)
    
    # Add JavaScript with better timing
    st.markdown(
        f"""
        <script>
        (function() {{
            function initTableSort() {{
                const table = document.getElementById('{table_id}');
                if (!table) {{
                    console.log('Table {table_id} not found, retrying...');
                    setTimeout(initTableSort, 200);
                    return;
                }}
                
                console.log('Initializing sort for table {table_id}');
                const headers = table.querySelectorAll('thead th');
                let currentSort = {{ col: -1, asc: true }};
                
                headers.forEach((header, index) => {{
                    header.addEventListener('click', function() {{
                        console.log('Clicked column', index);
                        const tbody = table.querySelector('tbody');
                        const rows = Array.from(tbody.querySelectorAll('tr'));
                        
                        // Determine sort direction
                        const asc = currentSort.col === index ? !currentSort.asc : true;
                        currentSort = {{ col: index, asc: asc }};
                        
                        // Remove sort classes from all headers
                        headers.forEach(h => {{
                            h.classList.remove('sort-asc', 'sort-desc');
                        }});
                        
                        // Add sort class to current header
                        header.classList.add(asc ? 'sort-asc' : 'sort-desc');
                        
                        // Sort rows
                        rows.sort((a, b) => {{
                            const aCell = a.cells[index];
                            const bCell = b.cells[index];
                            
                            if (!aCell || !bCell) return 0;
                            
                            // Get text content, removing % and other formatting
                            let aVal = aCell.textContent.trim().replace('%', '').replace(',', '');
                            let bVal = bCell.textContent.trim().replace('%', '').replace(',', '');
                            
                            // Try to parse as number
                            const aNum = parseFloat(aVal);
                            const bNum = parseFloat(bVal);
                            
                            // If both are numbers, compare numerically
                            if (!isNaN(aNum) && !isNaN(bNum)) {{
                                return asc ? aNum - bNum : bNum - aNum;
                            }}
                            
                            // Otherwise compare as strings
                            if (aVal < bVal) return asc ? -1 : 1;
                            if (aVal > bVal) return asc ? 1 : -1;
                            return 0;
                        }});
                        
                        // Re-append sorted rows
                        rows.forEach(row => tbody.appendChild(row));
                    }});
                }});
                
                console.log('Sort initialized for', headers.length, 'columns');
            }}
            
            // Try multiple times with increasing delays
            setTimeout(initTableSort, 100);
            setTimeout(initTableSort, 500);
            setTimeout(initTableSort, 1000);
        }})();
        </script>
        """,
        unsafe_allow_html=True
    )


