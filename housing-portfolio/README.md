# 🏠 Housing Market Analysis Portfolio

A comprehensive data analysis project examining U.S. housing market trends from 2000-2025, demonstrating proficiency in **SQL**, **Python**, and **Excel** for data analysis.

## 📊 Project Overview

This project analyzes home value index data across all U.S. states and cities to identify trends, growth patterns, and market insights. The analysis is implemented using multiple tools to showcase versatility in data analysis platforms.

### Key Questions Analyzed:
1. **Q1**: What are the yearly average home value indices by state?
2. **Q2**: Which 5 states showed the highest growth from 2000-2025?
3. **Q3**: Which 5 cities showed the highest growth from 2000-2025?
4. **Q4**: How many cities and counties are tracked in each state?

## 🛠️ Technical Skills Demonstrated

### SQL (DuckDB/PostgreSQL)
- ✅ Complex queries with CTEs and window functions
- ✅ Data aggregation and grouping
- ✅ Growth calculations and percentage analysis
- ✅ Multi-table joins and data relationships

### Python (pandas, matplotlib)
- ✅ Data manipulation and analysis
- ✅ Professional data visualization
- ✅ Automated report generation
- ✅ Statistical analysis and insights

### Excel (Advanced)
- ✅ Multi-sheet workbook organization
- ✅ Advanced formulas and calculations
- ✅ Pivot tables and data analysis
- ✅ Professional data presentation

## 📁 Project Structure

```
housing-portfolio/
├── data/                          # Raw data files
│   ├── home_values_yearly_clean.csv
│   └── housing.duckdb
├── sql/                           # SQL queries
│   └── analysis.sql
├── scripts/                       # Python analysis scripts
│   ├── run_analysis.py
│   └── create_excel_workbooks.py
├── excel/                         # Excel workbooks
│   ├── Q1_Top10_States_Average_Values.xlsx
│   ├── Q2_Top5_Home_Values_Growth.xlsx
│   ├── Q3_Top5_Cities_Growth_Analysis.xlsx
│   ├── Q4_State_Coverage_Analysis.xlsx
│   └── README.md
├── reports/                       # Analysis outputs
│   ├── figures/                   # Charts and visualizations
│   ├── *.csv                      # Data exports
│   └── summary.txt                # Key findings
└── requirements.txt               # Python dependencies
```

## 🚀 How to Run the Analysis

### Prerequisites
```bash
pip install -r requirements.txt
```

### Run Complete Analysis
```bash
python scripts/run_analysis.py
```

### Generate Excel Workbooks
```bash
python scripts/create_excel_workbooks.py
```

## 📈 Key Findings

### Q1: Yearly Trends
- **Top performing states** consistently show high home value indices
- **Clear growth patterns** visible across all states from 2000-2025
- **Market recovery** evident post-2008 financial crisis

### Q2: State Growth Leaders
- **Nevada** leads with highest growth percentage
- **Arizona** and **Florida** show strong growth patterns
- **Growth rates** range from 200-400% over 25 years

### Q3: City Growth Champions
- **Las Vegas, NV** tops city growth rankings
- **Phoenix, AZ** and **Miami, FL** follow closely
- **Urban centers** show more dramatic growth than rural areas

### Q4: Data Coverage
- **California** has the most comprehensive coverage
- **Texas** and **Florida** follow with extensive city/county tracking
- **Data quality** varies significantly by state

## 📊 Visualizations

All charts use a **consistent color scheme** for professional presentation:
- **Green** (`#2E7D32`): Primary data series
- **Blue** (`#1976D2`): Secondary data series
- **Extended palette**: 10 colors for multi-series charts

### Chart Types:
- **Line Charts**: Trend analysis over time
- **Bar Charts**: Comparative analysis
- **Grouped Bars**: Multi-category comparisons

## 💼 Business Impact

This analysis provides valuable insights for:
- **Real Estate Investors**: Identifying high-growth markets
- **Policy Makers**: Understanding regional housing trends
- **Urban Planners**: Planning for population and housing needs
- **Financial Institutions**: Risk assessment for mortgage lending

## 🔧 Technical Implementation

### Data Processing Pipeline
1. **Data Loading**: CSV import to DuckDB
2. **Data Cleaning**: Null value handling and validation
3. **Analysis**: SQL queries for insights
4. **Visualization**: Python matplotlib charts
5. **Export**: Excel workbooks and CSV reports

### Performance Optimizations
- **DuckDB**: Fast analytical queries
- **Pandas**: Efficient data manipulation
- **Vectorized operations**: Optimized calculations
- **Memory management**: Large dataset handling

## 📚 Learning Outcomes

This project demonstrates:
- **End-to-end data analysis** workflow
- **Multi-tool proficiency** (SQL, Python, Excel)
- **Professional visualization** techniques
- **Business insight generation** from raw data
- **Reproducible analysis** with clear documentation

---

*This portfolio showcases comprehensive data analysis skills across multiple platforms, demonstrating versatility and depth in modern data analysis tools and techniques.*

