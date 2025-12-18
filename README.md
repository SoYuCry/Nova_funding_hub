# Nova Funding Hub

**Nova Funding Hub** 是 **Nova 社区** 推出的一款多交易所资金费率监控工具，专为套利交易者打造。

🌐 **Live Demo**: [nova-btc.xyz](https://nova-btc.xyz/)

它实时聚合了各大去中心化交易所 (DEX) 和中心化交易所 (CEX) 的资金费率数据，帮助您快速发现跨平台套利机会，最大化资金利用率。

## ✨ 核心功能

*   **全平台监控**：支持 Aster, Backpack, Binance, EdgeX, Hyperliquid, Lighter, Paradex 等主流 DEX 和 CEX 交易所。
*   **智能套利发现**：
    *   **Max Spread APY**: 自动计算并高亮显示跨交易所的最大年化价差。
    *   **资金费率年化**: 自动将不同结算周期的费率统一转换为 APY，便于直观比较。
*   **极简高效 UI**：
    *   **实时刷新**: 后台每 60 秒自动更新数据，捕捉稍纵即逝的机会。
    *   **交互式表格**: 支持一键排序，快速筛选目标资产。
*   **健壮的降级**：
    *   接口异常时自动 fallback 缓存，页面不中断。
    *   结算周期（interval）按 `nextFundingTime` 推进推断；重启后需等下一次结算跳变才会刷新部分周期，初始可能短暂使用缓存/默认值。

## 🛠️ 安装与运行

### 前置要求

*   Python 3.8+

### 快速开始

1.  **安装依赖**

    ```bash
    pip install -r requirements.txt
    ```

2.  **启动应用**

    ```bash
    streamlit run app.py
    ```

3.  **访问面板**
    打开浏览器访问 `http://localhost:8501`。

## 📂 项目结构

*   `app.py`: 应用入口，负责 UI 渲染与交互。
*   `funding_core.py`: 数据核心层，处理多交易所 API 聚合与清洗。
*   `ui_components.py`: 定制化 UI 组件库。
*   `exchanges/`: 各交易所接口实现。


## 🚀 关于 Nova

Nova 是一个专注于 **加密货币套利 (Arbitrage)** 与 **量化交易** 的新兴社区。我们致力于分享高质量的套利策略、开发实用的交易工具，并连接志同道合的交易者。

🤝 **加入我们：**

*   **X (Twitter)**: [@0xYuCry](https://x.com/0xYuCry)
*   **Telegram**: [Nova Community](https://t.me/Nova_Crpytohub)
