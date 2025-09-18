#!/usr/bin/env python3
"""
Runs the portfolio pipeline:
- Imports the CSV into a DuckDB DB (data/housing.duckdb).
- Executes canonical SQL analyses.
- Saves tidy CSVs and simple charts to reports/.
"""
import argparse
from pathlib import Path
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data"
REPORTS = REPO / "reports"
FIGS = REPORTS / "figures"

def ensure_dirs():
    REPORTS.mkdir(parents=True, exist_ok=True)
    FIGS.mkdir(parents=True, exist_ok=True)

def load_into_duckdb(csv_path: Path) -> duckdb.DuckDBPyConnection:
    db_path = DATA / "housing.duckdb"
    con = duckdb.connect(str(db_path))
    # Create table and import CSV (use sample if full file missing)
    if not csv_path.exists():
        print(f"[warn] {csv_path} not found; falling back to sample.")
        csv_path = DATA / "sample_home_values_yearly_clean.csv"
    con.execute("DROP TABLE IF EXISTS home_values_yearly_clean;")
    con.execute(f"""
        CREATE TABLE home_values_yearly_clean AS
        SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=true);
    """)
    return con

def df_to_csv(df: pd.DataFrame, name: str):
    out = REPORTS / f"{name}.csv"
    df.to_csv(out, index=False)
    print(f"[ok] wrote {out}")

def bar_chart(df: pd.DataFrame, x: str, y: str, title: str, filename: str, top_n: int = 10):
    if top_n and len(df) > top_n:
        df = df.sort_values(y, ascending=False).head(top_n)
    
    # Sort with highest values on the left (ascending=False)
    df = df.sort_values(y, ascending=False)
    
    plt.figure(figsize=(12, 6))
    bars = plt.bar(df[x], df[y], color="#2E7D32")
    plt.title(title, fontsize=14, fontweight="bold")
    plt.xlabel("State", fontsize=12)
    plt.ylabel("Percent (%)", fontsize=12)
    plt.xticks(rotation=45, ha="right")
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{height:.1f}%', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    out = FIGS / f"{filename}.png"
    plt.savefig(out, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"[ok] wrote {out}")

def run_queries(con: duckdb.DuckDBPyConnection):
    # Q1: Which states have experienced the fastest and slowest growth 
    # in housing values since 2000?
    q1 = """
    WITH state_values AS (
        SELECT
            statename,
            year,
            ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
        FROM home_values_yearly_clean
        GROUP BY statename, year
    ),
    growth_calc AS (
        SELECT
            statename,
            MIN(CASE WHEN year = 2000 THEN avg_yearly_index END) AS value_2000,
            MAX(CASE WHEN year = 2025 THEN avg_yearly_index END) AS value_2025
        FROM state_values
        GROUP BY statename
    )
    SELECT
        statename,
        value_2000,
        value_2025,
        ROUND(((value_2025 - value_2000) / value_2000) * 100, 2) AS pct_growth
    FROM growth_calc
    WHERE value_2000 IS NOT NULL AND value_2025 IS NOT NULL
    ORDER BY pct_growth DESC;
    """
    df1 = con.execute(q1).df()
    df_to_csv(df1, "fastest_growth_since_2000")
    bar_chart(df1, "statename", "pct_growth", "Home Value Index Fastest Growth Since 2000 (Top 10)", "fastest_growth_top10")

    # Q2: Has the gap between the most expensive and least expensive states 
    # widened from 2000 to 2025?
    q4 = """
    WITH state_values AS (
        SELECT
            statename,
            year,
            ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
        FROM home_values_yearly_clean
        GROUP BY statename, year
    ),
    gap_analysis AS (
        SELECT
            year,
            MAX(avg_yearly_index) AS max_value,
            MIN(avg_yearly_index) AS min_value,
            MAX(avg_yearly_index) - MIN(avg_yearly_index) AS gap,
            ROUND(((MAX(avg_yearly_index) - MIN(avg_yearly_index)) / MIN(avg_yearly_index)) * 100, 2) AS gap_pct
        FROM state_values
        WHERE year IN (2000, 2025)
        GROUP BY year
    )
    SELECT
        year,
        max_value,
        min_value,
        gap,
        gap_pct,
        CASE 
            WHEN year = 2000 THEN 'Baseline'
            WHEN year = 2025 THEN 
                CASE 
                    WHEN gap > LAG(gap) OVER (ORDER BY year) THEN 'Gap Widened'
                    WHEN gap < LAG(gap) OVER (ORDER BY year) THEN 'Gap Narrowed'
                    ELSE 'Gap Unchanged'
                END
        END AS gap_trend
    FROM gap_analysis
    ORDER BY year;
    """
    df2 = con.execute(q4).df()
    df_to_csv(df2, "gap_analysis_2000_2025")
    
    # Create a simple comparison chart for the gap
    if len(df2) >= 2:
        plt.figure(figsize=(10, 6))
        years = df2['year'].astype(str)
        gaps = df2['gap']
        
        bars = plt.bar(years, gaps, color=['#1f77b4', '#ff7f0e'])
        plt.title('Gap Between Most and Least Expensive States: 2000 vs 2025', fontsize=14, fontweight='bold')
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Gap (Index Points)', fontsize=12)
        
        # Add value labels on bars
        for bar, gap in zip(bars, gaps):
            plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 5,
                    f'{gap:.1f}', ha='center', va='bottom', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        out = FIGS / "gap_comparison_2000_2025.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")

def write_summary():
    """Generate reports/summary.txt from the computed CSV outputs."""
    try:
        growth = pd.read_csv(REPORTS / "fastest_growth_since_2000.csv")
    except FileNotFoundError:
        growth = pd.DataFrame()
    try:
        gap = pd.read_csv(REPORTS / "gap_analysis_2000_2025.csv")
    except FileNotFoundError:
        gap = pd.DataFrame()

    # Compute findings with fallbacks
    highest_growth_line = "N/A"
    if not growth.empty and {"statename", "pct_growth"}.issubset(growth.columns):
        top_growth = growth.sort_values("pct_growth", ascending=False).head(1).iloc[0]
        highest_growth_line = f"{top_growth['statename']} ({top_growth['pct_growth']:.2f}% since 2000)"


    gap_verdict_line = "N/A"
    if not gap.empty and {"year", "gap", "gap_trend"}.issubset(gap.columns) and len(gap) >= 2:
        gap_2000 = float(gap.loc[gap["year"] == 2000, "gap"].iloc[0]) if not gap.loc[gap["year"] == 2000].empty else None
        gap_2025 = float(gap.loc[gap["year"] == 2025, "gap"].iloc[0]) if not gap.loc[gap["year"] == 2025].empty else None
        gap_trend = gap.loc[gap["year"] == 2025, "gap_trend"].iloc[0] if not gap.loc[gap["year"] == 2025].empty else "Unknown"
        if gap_2000 is not None and gap_2025 is not None:
            gap_verdict_line = f"{gap_trend} (2000: {gap_2000:,.2f} → 2025: {gap_2025:,.2f})"

    REPORTS.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS / "summary.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("Housing Portfolio Summary\n")
        f.write("==========================\n\n")
        f.write("- Highest growth since 2000: " + highest_growth_line + "\n")
        f.write("- Gap 2000 vs 2025: " + gap_verdict_line + "\n")
    print(f"[ok] wrote {out_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, default="data/home_values_yearly_clean.csv",
                        help="Path to the full CSV. Falls back to data/sample_home_values_yearly_clean.csv if missing.")
    args = parser.parse_args()
    ensure_dirs()
    con = load_into_duckdb(Path(args.data))
    run_queries(con)
    write_summary()
    print("[done] Analysis complete. See reports/ and reports/figures/.")

if __name__ == "__main__":
    main()
