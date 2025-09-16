# Housing Value Analysis (2000–2025)

## 📑 Overview
This project analyzes **US state-level housing values** between 2000 and 2025.  
It highlights long-term growth, the impact of the 2008 financial crisis, volatility, and affordability gaps.

---

## 🛠 Methods
- Cleaned dataset (~43 MB, ~680,000 rows)  
- SQL queries using **DuckDB** (`sql/analysis.sql`)  
- Automated analysis with **Python** (`scripts/run_analysis.py`)  
- Reports generated as CSVs and charts (`reports/`)

---

## 🔍 Key Questions
- Which states experienced the **fastest housing value growth** since 2000?  
- Which states were **hardest hit during the 2008 housing crisis**?  
- Which states show the **highest volatility** in housing values?  
- Has the **gap widened** between the most and least expensive states?  

---

## 📊 Findings
- **Fastest growth since 2000:** [Replace with result from reports/fastest_growth_since_2000.csv]  
- **Hardest hit 2007–2009:** [Replace with result from reports/hardest_hit_2007_2009.csv]  
- **Most volatile states:** [Replace with result from reports/volatility_by_state.csv]  
- **Gap between expensive vs. affordable states:** [Replace with result from reports/gap_2000_vs_2025.csv]  

---

## 📈 Visuals
![Fastest Growth Top 10](reports/figures/fastest_growth_top10.png)  
![Hardest Hit 2007–2009](reports/figures/hardest_hit_2007_2009_worst10.png)  
![Volatility Top 10](reports/figures/volatility_top10.png)

---

## 📑 Dataset
- **Full dataset:** `home_values_yearly_clean.csv` (~43 MB, ~680,000 rows) — tracked with *Git LFS*.  
- **Sample dataset:** `sample_home_values_yearly_clean.csv` (~60 KB) — included for quick inspection on GitHub.  

All analysis was run on the **full dataset**; the sample is for convenience.  

---
