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
import matplotlib.ticker as mtick

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

def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    filename: str,
    top_n: int = 10,
    orientation: str = "horizontal",
):
    # Sort, take Top-N, and reverse for horizontal barh (largest at top)
    df_plot = df.sort_values(y, ascending=False).head(top_n).iloc[::-1]

    # Use a clean, professional style
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(11, 7), dpi=160)

    if orientation == "vertical":
        bars = ax.bar(df_plot[x], df_plot[y], color="#2E7D32")
        ax.set_title(title, fontsize=16, fontweight="bold")
        ax.set_xlabel("State", fontsize=12)
        ax.tick_params(axis="x", labelsize=10, rotation=45)
    else:
        bars = ax.barh(df_plot[x], df_plot[y], color="#2E7D32")
        ax.set_title(title, fontsize=16, fontweight="bold")
        ax.set_ylabel("State", fontsize=12)
        ax.tick_params(axis="y", labelsize=11)

    # If y looks like a percent metric, format axis and labels accordingly
    is_percent = ("pct" in y.lower()) or ("%" in title)
    if is_percent:
        ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=100))
        value_fmt = lambda v: f"{v:.1f}%"
        ax.set_xlabel("Percent", fontsize=11)
    else:
        value_fmt = lambda v: f"{v:,.2f}"

    # Subtle gridlines
    if orientation == "vertical":
        ax.grid(axis="y", linestyle="--", alpha=0.3)
        ax.grid(False, axis="x")
    else:
        ax.grid(axis="x", linestyle="--", alpha=0.3)
        ax.grid(False, axis="y")

    # Annotate values at bar ends
    for bar in bars:
        if orientation == "vertical":
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2,
                    height + (max(df_plot[y]) * 0.01),
                    value_fmt(height),
                    va="bottom", ha="center", fontsize=10, color="#333333", rotation=0)
        else:
            width = bar.get_width()
            ax.text(width + (max(df_plot[y]) * 0.01),
                    bar.get_y() + bar.get_height() / 2,
                    value_fmt(width),
                    va="center", ha="left", fontsize=10, color="#333333")

    # Ensure enough left margin for long state names
    fig.tight_layout()
    if orientation != "vertical":
        fig.subplots_adjust(left=0.28)
    out = FIGS / f"{filename}.png"
    fig.savefig(out, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"[ok] wrote {out}")

def run_queries(con: duckdb.DuckDBPyConnection):
    # 1) Fastest growth since 2000
    q1 = """
    WITH state_year AS (
      SELECT statename, year, AVG(yearlyindex) AS avg_index
      FROM home_values_yearly_clean
      WHERE yearlyindex IS NOT NULL
      GROUP BY 1,2
    ),
    growth_base AS (
      SELECT
        statename,
        MAX(CASE WHEN year = 2000 THEN avg_index END) AS idx_2000,
        MAX(CASE WHEN year = 2025 THEN avg_index END) AS idx_2025
      FROM state_year
      GROUP BY 1
    )
    SELECT
      statename,
      ROUND(idx_2000, 2) AS idx_2000,
      ROUND(idx_2025, 2) AS idx_2025,
      ROUND(((idx_2025 - idx_2000) / idx_2000) * 100, 2) AS pct_growth
    FROM growth_base
    WHERE idx_2000 IS NOT NULL AND idx_2025 IS NOT NULL
    ORDER BY pct_growth DESC;
    """
    df1 = con.execute(q1).df()
    df_to_csv(df1, "fastest_growth_since_2000")
    bar_chart(
        df1,
        "statename",
        "pct_growth",
        "Fastest Growth Since 2000 (%), Top 10",
        "fastest_growth_top10",
        orientation="vertical",
    )

    # 2) Hardest hit 2007 -> 2009 (removed by request)

    # 3) Volatility (std dev of YoY % changes)
    q3 = """
    WITH state_year AS (
      SELECT statename, year, AVG(yearlyindex) AS avg_index
      FROM home_values_yearly_clean
      WHERE yearlyindex IS NOT NULL
      GROUP BY 1,2
    ),
    yoy AS (
      SELECT
        curr.statename,
        curr.year,
        (curr.avg_index - prev.avg_index) / NULLIF(prev.avg_index, 0) AS yoy_pct
      FROM state_year curr
      JOIN state_year prev
        ON curr.statename = prev.statename AND curr.year = prev.year + 1
      WHERE prev.avg_index IS NOT NULL
    )
    SELECT
      statename,
      ROUND(STDDEV_SAMP(yoy_pct) * 100, 2) AS yoy_volatility_pct
    FROM yoy
    GROUP BY 1
    ORDER BY yoy_volatility_pct DESC;
    """
    df3 = con.execute(q3).df()
    df_to_csv(df3, "volatility_by_state")
    bar_chart(
        df3,
        "statename",
        "yoy_volatility_pct",
        "YoY Volatility by State (%), Top 10",
        "volatility_top10",
        orientation="vertical",
    )

    # 4) Gap widened 2000 vs 2025
    q4 = """
    WITH state_year AS (
      SELECT statename, year, AVG(yearlyindex) AS avg_index
      FROM home_values_yearly_clean
      WHERE yearlyindex IS NOT NULL
      GROUP BY 1,2
    ),
    gap_by_year AS (
      SELECT year, MAX(avg_index) - MIN(avg_index) AS gap
      FROM state_year
      GROUP BY year
    )
    SELECT year, ROUND(gap, 2) AS gap
    FROM gap_by_year
    WHERE year IN (2000, 2025)
    ORDER BY year;
    """
    df4 = con.execute(q4).df()
    df_to_csv(df4, "gap_2000_vs_2025")

def write_summary():
    """Generate reports/summary.txt from the computed CSV outputs."""
    try:
        growth = pd.read_csv(REPORTS / "fastest_growth_since_2000.csv")
    except FileNotFoundError:
        growth = pd.DataFrame()
    try:
        crisis = pd.read_csv(REPORTS / "hardest_hit_2007_2009.csv")
    except FileNotFoundError:
        crisis = pd.DataFrame()
    try:
        vol = pd.read_csv(REPORTS / "volatility_by_state.csv")
    except FileNotFoundError:
        vol = pd.DataFrame()
    try:
        gap = pd.read_csv(REPORTS / "gap_2000_vs_2025.csv")
    except FileNotFoundError:
        gap = pd.DataFrame()

    # Compute findings with fallbacks
    highest_growth_line = "N/A"
    if not growth.empty and {"statename", "pct_growth"}.issubset(growth.columns):
        top_growth = growth.sort_values("pct_growth", ascending=False).head(1).iloc[0]
        highest_growth_line = f"{top_growth['statename']} ({top_growth['pct_growth']:.2f}% since 2000)"

    hardest_hit_line = "N/A"
    if not crisis.empty and {"statename", "pct_change_07_09"}.issubset(crisis.columns):
        worst = crisis.sort_values("pct_change_07_09", ascending=True).head(1).iloc[0]
        hardest_hit_line = f"{worst['statename']} ({worst['pct_change_07_09']:.2f}% from 2007 to 2009)"

    most_volatile_line = "N/A"
    if not vol.empty and {"statename", "yoy_volatility_pct"}.issubset(vol.columns):
        mv = vol.sort_values("yoy_volatility_pct", ascending=False).head(1).iloc[0]
        most_volatile_line = f"{mv['statename']} ({mv['yoy_volatility_pct']:.2f}% YoY volatility)"

    gap_verdict_line = "N/A"
    if not gap.empty and {"year", "gap"}.issubset(gap.columns) and len(gap) >= 2:
        gap_2000 = float(gap.loc[gap["year"] == 2000, "gap"].iloc[0]) if not gap.loc[gap["year"] == 2000].empty else None
        gap_2025 = float(gap.loc[gap["year"] == 2025, "gap"].iloc[0]) if not gap.loc[gap["year"] == 2025].empty else None
        if gap_2000 is not None and gap_2025 is not None:
            widened = gap_2025 > gap_2000
            direction = "widened" if widened else ("narrowed or unchanged")
            gap_verdict_line = f"{direction} (2000: {gap_2000:,.2f} → 2025: {gap_2025:,.2f})"

    REPORTS.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS / "summary.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("Housing Portfolio Summary\n")
        f.write("==========================\n\n")
        f.write("- Highest growth since 2000: " + highest_growth_line + "\n")
        f.write("- Hardest hit in 2007–2009: " + hardest_hit_line + "\n")
        f.write("- Most volatile state: " + most_volatile_line + "\n")
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
