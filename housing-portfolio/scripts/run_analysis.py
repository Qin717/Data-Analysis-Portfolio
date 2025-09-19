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
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(df[x], df[y], color="#2E7D32", alpha=0.8, edgecolor='white', linewidth=1)
    plt.title(title, fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("State", fontsize=12, fontweight="bold")
    plt.ylabel("Growth Percentage (%)", fontsize=12, fontweight="bold")
    plt.xticks(rotation=45, ha="right", fontsize=10)
    plt.yticks(fontsize=10)
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                f'{height:.1f}%', ha='center', va='bottom', fontsize=10, fontweight="bold")
    
    # Add subtle grid
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    out = FIGS / f"{filename}.png"
    plt.savefig(out, dpi=200, bbox_inches="tight")
    plt.close()
    print(f"[ok] wrote {out}")

def run_queries(con: duckdb.DuckDBPyConnection):
    # Q1: Calculate the yearly average home value index for each state
    q1 = """
    SELECT
        statename,
        year,
        ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
    FROM home_values_yearly_clean
    WHERE yearlyindex IS NOT NULL
    GROUP BY statename, year
    ORDER BY statename, year;
    """
    df1 = con.execute(q1).df()
    
    # Create pivot format for Q1 (matching Excel format)
    if not df1.empty:
        # Use the exact order from Excel file: CA, CO, CT, DC, HI, MA, MD, NJ, UT, WA
        excel_order = ['CA', 'CO', 'CT', 'DC', 'HI', 'MA', 'MD', 'NJ', 'UT', 'WA']
        
        # Filter to these specific states only
        df_top10 = df1[df1['statename'].isin(excel_order)]
        
        # Create pivot table: years as rows, states as columns
        pivot_df = df_top10.pivot(index='year', columns='statename', values='avg_yearly_index')
        
        # Reorder columns to match the Excel order exactly
        pivot_df = pivot_df[excel_order]
        
        # Round to whole numbers (no decimals)
        pivot_df = pivot_df.round(0).astype(int)
        
        # Format numbers with commas for better readability
        pivot_df_formatted = pivot_df.copy()
        for col in pivot_df_formatted.columns:
            pivot_df_formatted[col] = pivot_df_formatted[col].apply(lambda x: f"{x:,}")
        
        # Save the pivot CSV with comma formatting
        out = REPORTS / "Q1_Top10_States_Average_Values.csv"
        pivot_df_formatted.to_csv(out)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df1, "Q1_Top10_States_Average_Values")
    
    # Create a line chart for Q1 showing trends over time
    if not df1.empty:
        # Use the exact same order as Excel file
        excel_order = ['CA', 'CO', 'CT', 'DC', 'HI', 'MA', 'MD', 'NJ', 'UT', 'WA']
        df1_sample = df1[df1['statename'].isin(excel_order)]
        
        plt.figure(figsize=(14, 8))
        colors = ['#2E7D32', '#1976D2', '#D32F2F', '#F57C00', '#7B1FA2', '#388E3C', '#303F9F', '#C2185B', '#FBC02D', '#5D4037']
        for i, state in enumerate(excel_order):
            state_data = df1_sample[df1_sample['statename'] == state]
            plt.plot(state_data['year'], state_data['avg_yearly_index'], marker='o', label=state, linewidth=2, color=colors[i % len(colors)])
        
        plt.title('Top 10 States by Yearly Average Values Index (2000-2025)', fontsize=14, fontweight='bold')
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Average Home Value Index', fontsize=12)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        out = FIGS / "Q1_Top10_States_Average_Values.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")
    
    # Q2: Which 5 states have shown the highest growth in home values index from 2000 to 2025?
    q2 = """
    WITH state_values AS (
        SELECT
            statename,
            year,
            ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
        FROM home_values_yearly_clean
        WHERE yearlyindex IS NOT NULL
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
    ORDER BY pct_growth DESC
    LIMIT 5;
    """
    df2 = con.execute(q2).df()
    
    # Format Q2 data for better presentation
    if not df2.empty:
        # Add formatted columns for better readability
        df2_formatted = df2.copy()
        df2_formatted['value_2000_formatted'] = df2_formatted['value_2000'].apply(lambda x: f"${x:,.2f}")
        df2_formatted['value_2025_formatted'] = df2_formatted['value_2025'].apply(lambda x: f"${x:,.2f}")
        df2_formatted['pct_growth_formatted'] = df2_formatted['pct_growth'].apply(lambda x: f"{x:.2f}%")
        
        # Reorder columns for better presentation
        df2_formatted = df2_formatted[['statename', 'value_2000_formatted', 'value_2025_formatted', 'pct_growth_formatted', 'value_2000', 'value_2025', 'pct_growth']]
        df2_formatted.columns = ['State', 'Value_2000_Formatted', 'Value_2025_Formatted', 'Growth_Formatted', 'Value_2000_Raw', 'Value_2025_Raw', 'Growth_Raw']
        
        out = REPORTS / "Q2_Top5_Home_Values_Growth.csv"
        df2_formatted.to_csv(out, index=False)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df2, "q2_top5_states_highest_growth_2000_2025")
    # Create combo chart for Q2 (matching Excel format)
    if not df2.empty:
        # Calculate absolute growth
        df2['absolute_growth'] = df2['value_2025'] - df2['value_2000']
        
        # Create combo chart with bars and line
        fig, ax1 = plt.subplots(figsize=(12, 8))
        
        # Create bars for absolute growth (left y-axis) - Dark blue
        bars = ax1.bar(df2['statename'], df2['absolute_growth'], color='#1f4e79', alpha=0.8, width=0.6)
        ax1.set_xlabel('States', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Increase in Home Values ($)', fontsize=12, fontweight='bold', color='black')
        ax1.set_ylim(0, 700000)
        ax1.set_yticks(range(0, 700001, 100000))
        ax1.tick_params(axis='y', labelcolor='black')
        
        # Format left Y-axis tick labels with commas
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'${height:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold', color='black')
        
        # Create second y-axis for percentage growth
        ax2 = ax1.twinx()
        line = ax2.plot(df2['statename'], df2['pct_growth'], color='#FFD700', marker='o', 
                       linewidth=3, markersize=8, label='% Growth')
        ax2.set_ylabel('% Growth (2000-2025)', fontsize=12, fontweight='bold', color='black')
        ax2.set_ylim(0, 400)
        ax2.set_yticks(range(0, 401, 50))
        ax2.tick_params(axis='y', labelcolor='black')
        
        # Format right Y-axis tick labels with % symbol
        ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0f}%'))
        
        # Add percentage labels on line points
        for i, (state, pct) in enumerate(zip(df2['statename'], df2['pct_growth'])):
            ax2.text(i, pct + 10, f'{pct:.2f}%', ha='center', va='bottom',
                    fontsize=10, fontweight='bold', color='black')
        
        # Set title
        plt.title('Top 5 States: Home Value Growth (2000-2025)', fontsize=16, fontweight='bold', pad=20)
        
        # Create custom legend
        from matplotlib.patches import Patch
        from matplotlib.lines import Line2D
        legend_elements = [Patch(facecolor='#1f4e79', label='Absolute Growth'),
                          Line2D([0], [0], color='#FFD700', linewidth=3, marker='o', label='% Growth')]
        ax1.legend(handles=legend_elements, loc='lower center', bbox_to_anchor=(0.5, -0.15), ncol=2)
        
        # Add grid
        ax1.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        out = FIGS / "Q2_Top5_Home_Values_Growth.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")
    
    # Q3: Which top 5 cities have shown the highest growth in home value index from 2000 to 2025?
    q3 = """
    WITH city_values AS (
        SELECT
            city,
            statename,
            year,
            ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
        FROM home_values_yearly_clean
        WHERE yearlyindex IS NOT NULL
        GROUP BY city, statename, year
    ),
    city_2000 AS (
        SELECT
            city,
            statename,
            avg_yearly_index AS value_2000
        FROM city_values
        WHERE year = 2000
    ),
    city_2025 AS (
        SELECT
            city,
            statename,
            avg_yearly_index AS value_2025
        FROM city_values
        WHERE year = 2025
    )
    SELECT
        c2000.city,
        c2000.statename,
        c2000.value_2000,
        c2025.value_2025,
        ROUND(((c2025.value_2025 - c2000.value_2000) / c2000.value_2000) * 100, 2) AS pct_growth
    FROM city_2000 c2000
    JOIN city_2025 c2025 ON c2000.city = c2025.city AND c2000.statename = c2025.statename
    ORDER BY pct_growth DESC
    LIMIT 5;
    """
    df3 = con.execute(q3).df()
    
    # Format Q3 data for better presentation
    if not df3.empty:
        # Add formatted columns for better readability
        df3_formatted = df3.copy()
        df3_formatted['value_2000_formatted'] = df3_formatted['value_2000'].apply(lambda x: f"${x:,.2f}")
        df3_formatted['value_2025_formatted'] = df3_formatted['value_2025'].apply(lambda x: f"${x:,.2f}")
        df3_formatted['pct_growth_formatted'] = df3_formatted['pct_growth'].apply(lambda x: f"{x:.2f}%")
        df3_formatted['city_state'] = df3_formatted['city'] + ', ' + df3_formatted['statename']
        
        # Reorder columns for better presentation
        df3_formatted = df3_formatted[['city_state', 'city', 'statename', 'value_2000_formatted', 'value_2025_formatted', 'pct_growth_formatted', 'value_2000', 'value_2025', 'pct_growth']]
        df3_formatted.columns = ['City_State', 'City', 'State', 'Value_2000_Formatted', 'Value_2025_Formatted', 'Growth_Formatted', 'Value_2000_Raw', 'Value_2025_Raw', 'Growth_Raw']
        
        out = REPORTS / "Q3_Top5_Cities_Highest_Growth_2000_2025.csv"
        df3_formatted.to_csv(out, index=False)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df3, "q3_top5_cities_highest_growth_2000_2025")
    
    # Create a horizontal bar chart for Q3 with city names (different from states)
    if not df3.empty:
        # Create a combined city-state label for better readability
        df3['city_state'] = df3['city'] + ', ' + df3['statename']
        
        # Sort with highest values at the top
        df3 = df3.sort_values('pct_growth', ascending=True)
        
        plt.figure(figsize=(10, 6))
        bars = plt.barh(df3['city_state'], df3['pct_growth'], color="#1976D2", alpha=0.8, edgecolor='white', linewidth=1)
        
        plt.title('Top 5 Cities with Highest Growth in Home Values (2000-2025)', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Growth Percentage (%)', fontsize=12, fontweight='bold')
        plt.ylabel('City, State', fontsize=12, fontweight='bold')
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        
        # Add value labels on the right side of bars
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                    f'{width:.1f}%', ha='left', va='center', fontsize=10, fontweight='bold')
        
        # Add grid for better readability
        plt.grid(axis='x', alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        out = FIGS / "q3_top5_cities_highest_growth.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")
    
    # Q4: How many cities & counties are in each state?
    q4 = """
    SELECT
        statename,
        COUNT(DISTINCT city) AS unique_cities,
        COUNT(DISTINCT countyname) AS unique_counties
    FROM home_values_yearly_clean
    GROUP BY statename
    ORDER BY unique_cities DESC;
    """
    df4 = con.execute(q4).df()
    
    # Format Q4 data for better presentation
    if not df4.empty:
        # Add formatted columns for better readability
        df4_formatted = df4.copy()
        df4_formatted['unique_cities_formatted'] = df4_formatted['unique_cities'].apply(lambda x: f"{x:,}")
        df4_formatted['unique_counties_formatted'] = df4_formatted['unique_counties'].apply(lambda x: f"{x:,}")
        
        # Reorder columns for better presentation
        df4_formatted = df4_formatted[['statename', 'unique_cities_formatted', 'unique_counties_formatted', 'unique_cities', 'unique_counties']]
        df4_formatted.columns = ['State', 'Cities_Formatted', 'Counties_Formatted', 'Cities_Raw', 'Counties_Raw']
        
        out = REPORTS / "Q4_Cities_Counties_by_State.csv"
        df4_formatted.to_csv(out, index=False)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df4, "q4_cities_counties_by_state")
    
    # Create a bar chart for Q4 showing top 10 states by number of cities
    if not df4.empty:
        # Get top 10 states by number of cities
        df4_top10 = df4.head(10)
        
        plt.figure(figsize=(12, 8))
        
        # Create grouped bar chart
        states = df4_top10['statename']
        cities = df4_top10['unique_cities']
        counties = df4_top10['unique_counties']
        
        x = range(len(states))
        width = 0.35
        
        bars1 = plt.bar([i - width/2 for i in x], cities, width, label='Cities', color='#2E7D32', alpha=0.8, edgecolor='white', linewidth=1)
        bars2 = plt.bar([i + width/2 for i in x], counties, width, label='Counties', color='#1976D2', alpha=0.8, edgecolor='white', linewidth=1)
        
        plt.title('Top 10 States by Number of Cities and Counties Tracked', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('State', fontsize=12, fontweight='bold')
        plt.ylabel('Count', fontsize=12, fontweight='bold')
        plt.xticks(x, states, rotation=45, ha='right', fontsize=10)
        plt.yticks(fontsize=10)
        plt.legend(fontsize=10)
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                        f'{int(height)}', ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        out = FIGS / "q4_cities_counties_by_state.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")


def write_summary():
    """Generate reports/summary.txt from the computed CSV outputs."""
    try:
        yearly_avg = pd.read_csv(REPORTS / "q1_yearly_average_by_state.csv")
    except FileNotFoundError:
        yearly_avg = pd.DataFrame()
    try:
        growth = pd.read_csv(REPORTS / "q2_top5_states_highest_growth_2000_2025.csv")
    except FileNotFoundError:
        growth = pd.DataFrame()
    try:
        city_growth = pd.read_csv(REPORTS / "q3_top5_cities_highest_growth_2000_2025.csv")
    except FileNotFoundError:
        city_growth = pd.DataFrame()
    try:
        state_counts = pd.read_csv(REPORTS / "q4_cities_counties_by_state.csv")
    except FileNotFoundError:
        state_counts = pd.DataFrame()

    # Compute findings with fallbacks
    yearly_stats_line = "N/A"
    if not yearly_avg.empty and {"statename", "year", "avg_yearly_index"}.issubset(yearly_avg.columns):
        total_states = yearly_avg['statename'].nunique()
        year_range = f"{yearly_avg['year'].min()}-{yearly_avg['year'].max()}"
        yearly_stats_line = f"{total_states} states, {year_range} period"
    
    highest_growth_line = "N/A"
    if not growth.empty and {"statename", "pct_growth"}.issubset(growth.columns):
        top_growth = growth.sort_values("pct_growth", ascending=False).head(1).iloc[0]
        highest_growth_line = f"{top_growth['statename']} ({top_growth['pct_growth']:.2f}% growth)"
    
    highest_city_growth_line = "N/A"
    if not city_growth.empty and {"city", "statename", "pct_growth"}.issubset(city_growth.columns):
        top_city_growth = city_growth.sort_values("pct_growth", ascending=False).head(1).iloc[0]
        highest_city_growth_line = f"{top_city_growth['city']}, {top_city_growth['statename']} ({top_city_growth['pct_growth']:.2f}% growth)"
    
    state_counts_line = "N/A"
    if not state_counts.empty and {"statename", "unique_cities"}.issubset(state_counts.columns):
        top_state = state_counts.head(1).iloc[0]
        state_counts_line = f"{top_state['statename']} has the most cities ({top_state['unique_cities']})"

    REPORTS.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS / "summary.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("Housing Portfolio Summary\n")
        f.write("==========================\n\n")
        f.write("- Dataset coverage: " + yearly_stats_line + "\n")
        f.write("- Highest state growth 2000-2025: " + highest_growth_line + "\n")
        f.write("- Highest city growth 2000-2025: " + highest_city_growth_line + "\n")
        f.write("- Most cities tracked: " + state_counts_line + "\n")
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
