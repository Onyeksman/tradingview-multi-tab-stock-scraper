# 📈 TradingView Stock Data Scraper (Async Playwright + Excel)

A high-performance **Python scraper** that automatically collects and exports **TradingView stock data** into a clean, styled Excel report.  
Built with **Playwright (asyncio)** and **pandas**, it scrapes all key financial tabs—Overview, Valuation, Performance, Dividends, etc.—for data analysts, traders, and automation-focused businesses.

---

## ⚙️ Features
- 🚀 **Asynchronous scraping** with Playwright for ultra-fast data extraction  
- 🧭 Auto-navigation through all stock tabs  
- 📊 **Professional Excel formatting** (headers, alignment, auto-widths, % and $ formatting)  
- 💾 Smart pagination with full data capture  
- 🧹 Ticker & company name separation and cleanup  

---

## 🧠 Tech Stack
**Python**, **Playwright (async)**, **pandas**, **openpyxl**, **asyncio**

---

## 📊 Output Example
```markdown
**File:** `tradingview_all_us_stocks.xlsx`
**Tabs:** Overview, Performance, Valuation, Dividends, Profitability, Financials, Technicals

| Ticker | Company Name | Price | Market Cap | P/E | Yield |
|--------|---------------|-------|-------------|------|--------|
| AAPL | Apple Inc. | $228.52 | $3.5T | 36.2 | 0.45% |
| MSFT | Microsoft Corp. | $420.33 | $3.2T | 35.7 | 0.75% |
| NVDA | NVIDIA Corp. | $1,045.65 | $2.6T | 72.4 | 0.02% |

---
## 💼 Use Cases
- 📈 Market analysis & research dashboards  
- 🧾 Financial data collection automation  
- 💹 Trading and portfolio monitoring  
- 🧠 ML / AI stock data pipelines  

---

## 🏆 Impact

✅ Reduced manual collection from hours to minutes  
✅ 100% complete, cleanly formatted Excel output  
✅ Supports thousands of tickers asynchronously  
✅ Enables analysts to focus on insights, not scraping  



