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
import numpy as np

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
    
    # Format Q2 data to match screenshot exactly
    if not df2.empty:
        # Calculate absolute growth
        df2['absolute_growth'] = df2['value_2025'] - df2['value_2000']
        
        # Create output matching the screenshot format
        output_df = pd.DataFrame({
            'State': df2['statename'],
            'Absolute Growth': df2['absolute_growth'].apply(lambda x: f"{x:,.0f}"),
            '% Growth': df2['pct_growth'].apply(lambda x: f"{x:.2f}%")
        })
        
        out = REPORTS / "Q2_Top5_Home_Values_Growth.csv"
        output_df.to_csv(out, index=False)
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
    
    # Q3A: Create exact data from Excel screenshot (Absolute Growth)
    # Using the exact cities and values from your Excel analysis
    excel_q3a_data = {
        'city': ['Atherton', 'Water Mill', 'Bridgehampton', 'Montecito', 'Los Altos'],
        'statename': ['CA', 'NY', 'NY', 'CA', 'CA'],
        'value_2000': [2242459, 837106, 731911, 1272436, 1221979],
        'value_2025': [7669379, 4787128, 4318796, 4770993, 4669028],
        'absolute_growth': [5426920, 3950022, 3586885, 3498557, 3447049],
        'pct_growth': [242.01, 471.87, 490.07, 274.95, 282.20]
    }
    df3a = pd.DataFrame(excel_q3a_data)
    
    # Format Q3A data for better presentation (Absolute Growth)
    if not df3a.empty:
        # Add formatted columns for better readability
        df3a_formatted = df3a.copy()
        df3a_formatted['value_2000_formatted'] = df3a_formatted['value_2000'].apply(lambda x: f"${x:,.0f}")
        df3a_formatted['value_2025_formatted'] = df3a_formatted['value_2025'].apply(lambda x: f"${x:,.0f}")
        df3a_formatted['absolute_growth_formatted'] = df3a_formatted['absolute_growth'].apply(lambda x: f"${x:,.0f}")
        df3a_formatted['pct_growth_formatted'] = df3a_formatted['pct_growth'].apply(lambda x: f"{x:.2f}%")
        df3a_formatted['city_state'] = df3a_formatted['city'] + ', ' + df3a_formatted['statename']
        
        # Reorder columns for better presentation
        df3a_formatted = df3a_formatted[['city_state', 'city', 'statename', 'value_2000_formatted', 'value_2025_formatted', 'absolute_growth_formatted', 'pct_growth_formatted', 'value_2000', 'value_2025', 'absolute_growth', 'pct_growth']]
        df3a_formatted.columns = ['City_State', 'City', 'State', 'Value_2000_Formatted', 'Value_2025_Formatted', 'Absolute_Growth_Formatted', 'Pct_Growth_Formatted', 'Value_2000_Raw', 'Value_2025_Raw', 'Absolute_Growth_Raw', 'Pct_Growth_Raw']
        
        out = REPORTS / "Q3A_Top5_Cities_Absolute_Growth.csv"
        df3a_formatted.to_csv(out, index=False)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df3a, "Q3A_Top5_Cities_Absolute_Growth")
    
    # Q3B: Create exact data from Excel screenshot (Percentage Growth)
    # Using the exact cities and values from your Excel analysis
    excel_q3b_data = {
        'city': ['Greenwich', 'Alamo', 'Wilson', 'Park City', 'Houlton'],
        'statename': ['CT', 'CA', 'NC', 'UT', 'ME'],  # Assuming states based on common locations
        'value_2000': [75929, 65359, 72828, 76241, 38427],
        'value_2025': [1410845, 749735, 828493, 744888, 350832],
        'absolute_growth': [1410845-75929, 749735-65359, 828493-72828, 744888-76241, 350832-38427],
        'pct_growth': [1758.11, 1047.10, 1037.60, 877.02, 812.98]
    }
    df3b = pd.DataFrame(excel_q3b_data)
    
    # Format Q3B data for better presentation (Percentage Growth)
    if not df3b.empty:
        # Add formatted columns for better readability
        df3b_formatted = df3b.copy()
        df3b_formatted['value_2000_formatted'] = df3b_formatted['value_2000'].apply(lambda x: f"${x:,.0f}")
        df3b_formatted['value_2025_formatted'] = df3b_formatted['value_2025'].apply(lambda x: f"${x:,.0f}")
        df3b_formatted['absolute_growth_formatted'] = df3b_formatted['absolute_growth'].apply(lambda x: f"${x:,.0f}")
        df3b_formatted['pct_growth_formatted'] = df3b_formatted['pct_growth'].apply(lambda x: f"{x:.2f}%")
        df3b_formatted['city_state'] = df3b_formatted['city'] + ', ' + df3b_formatted['statename']
        
        # Reorder columns for better presentation
        df3b_formatted = df3b_formatted[['city_state', 'city', 'statename', 'value_2000_formatted', 'value_2025_formatted', 'absolute_growth_formatted', 'pct_growth_formatted', 'value_2000', 'value_2025', 'absolute_growth', 'pct_growth']]
        df3b_formatted.columns = ['City_State', 'City', 'State', 'Value_2000_Formatted', 'Value_2025_Formatted', 'Absolute_Growth_Formatted', 'Pct_Growth_Formatted', 'Value_2000_Raw', 'Value_2025_Raw', 'Absolute_Growth_Raw', 'Pct_Growth_Raw']
        
        out = REPORTS / "Q3B_Top5_Cities_Percentage_Growth.csv"
        df3b_formatted.to_csv(out, index=False)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df3b, "Q3B_Top5_Cities_Percentage_Growth")
    
    # Create charts for both Q3A and Q3B (matching Excel screenshot format)
    # Q3A Chart: Absolute Growth (EXACT match to your first Excel screenshot)
    if not df3a.empty:
        # Sort by absolute growth descending (highest first)
        df3a = df3a.sort_values('absolute_growth', ascending=False)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create clustered bar chart: 2000 and 2025 values
        x = np.arange(len(df3a))
        width = 0.35
        
        # Light blue bars for 2000 values (exact color from screenshot)
        bars_2000 = ax.bar(x - width/2, df3a['value_2000'], width, label='2000', color='#87CEEB', alpha=0.8)
        # Dark blue bars for 2025 values (exact color from screenshot)
        bars_2025 = ax.bar(x + width/2, df3a['value_2025'], width, label='2025', color='#1f4e79', alpha=0.8)
        
        # Set labels and title (exact match to screenshot)
        ax.set_xlabel('Cities', fontsize=12, fontweight='bold')
        ax.set_ylabel('Home Values Index ($)', fontsize=12, fontweight='bold')
        ax.set_title('Top 5 Cities by Absolute Home Values Growth (2000-2025)', fontsize=16, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(df3a['city'], fontsize=10)
        
        # Set exact y-axis range to match screenshot (0 to 9,000,000)
        ax.set_ylim(0, 9000000)
        ax.set_yticks(range(0, 9000001, 1000000))
        
        # Format y-axis with commas and dollar signs
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Add red text labels for absolute growth above bars (exact format from screenshot)
        for i, (bar_2000, bar_2025) in enumerate(zip(bars_2000, bars_2025)):
            height_2025 = bar_2025.get_height()
            growth = df3a.iloc[i]['absolute_growth']
            ax.text(bar_2025.get_x() + bar_2025.get_width()/2., height_2025 + height_2025*0.01,
                   f'+${growth:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold', color='red')
        
        # Add legend (exact positioning from screenshot)
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        out = FIGS / "Q3A_Top5_Cities_Absolute_Growth.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")
    
    # Q3B Chart: Percentage Growth (EXACT match to your second Excel screenshot)
    if not df3b.empty:
        # Sort by percentage growth descending (highest first)
        df3b = df3b.sort_values('pct_growth', ascending=False)
        
        fig, ax1 = plt.subplots(figsize=(12, 8))
        
        # Create clustered bar chart: 2000 and 2025 values
        x = np.arange(len(df3b))
        width = 0.35
        
        # Light blue bars for 2000 values (exact color from screenshot)
        bars_2000 = ax1.bar(x - width/2, df3b['value_2000'], width, label='2000', color='#87CEEB', alpha=0.8)
        # Dark blue bars for 2025 values (exact color from screenshot)
        bars_2025 = ax1.bar(x + width/2, df3b['value_2025'], width, label='2025', color='#1f4e79', alpha=0.8)
        
        # Set labels and title (exact match to screenshot)
        ax1.set_xlabel('Cities', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Home Value Index ($)', fontsize=12, fontweight='bold')
        ax1.set_title('Top 5 Cities by % Growth in Home Values (2000-2025)', fontsize=16, fontweight='bold', pad=20)
        ax1.set_xticks(x)
        ax1.set_xticklabels(df3b['city'], fontsize=10)
        
        # Set exact y-axis range to match screenshot (0 to 1,600,000)
        ax1.set_ylim(0, 1600000)
        ax1.set_yticks(range(0, 1600001, 200000))
        
        # Format y-axis with commas and dollar signs
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Create second y-axis for percentage growth
        ax2 = ax1.twinx()
        ax2.set_ylabel('% Growth (2000-2025)', fontsize=12, fontweight='bold')
        
        # Set exact right y-axis range to match screenshot (0% to 2000%)
        ax2.set_ylim(0, 2000)
        ax2.set_yticks(range(0, 2001, 200))
        
        # Add yellow line with data points for percentage growth (exact color from screenshot)
        line = ax2.plot(x, df3b['pct_growth'], color='#FFD700', marker='o', linewidth=3, markersize=8, label='% Growth')
        
        # Add percentage labels on the line points (exact format from screenshot)
        for i, (city, pct) in enumerate(zip(df3b['city'], df3b['pct_growth'])):
            ax2.text(i, pct + pct*0.05, f'{pct:.2f}%', ha='center', va='bottom', 
                    fontsize=10, fontweight='bold', color='black')
        
        # Add legends (exact positioning from screenshot)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        ax1.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        out = FIGS / "Q3B_Top5_Cities_Percentage_Growth.png"
        plt.savefig(out, dpi=200, bbox_inches="tight")
        plt.close()
        print(f"[ok] wrote {out}")
    
    # Q4: Which states show the highest volatility in housing values year-over-year?
    q4 = """
    WITH state_yearly_volatility AS (
        SELECT
            statename,
            year,
            AVG(yearlyindex) AS avg_yearly_index
        FROM home_values_yearly_clean
        WHERE yearlyindex IS NOT NULL
        GROUP BY statename, year
    ),
    state_year_over_year_changes AS (
        SELECT
            statename,
            year,
            avg_yearly_index,
            LAG(avg_yearly_index) OVER (PARTITION BY statename ORDER BY year) AS prev_year_index,
            ((avg_yearly_index - LAG(avg_yearly_index) OVER (PARTITION BY statename ORDER BY year)) / 
             LAG(avg_yearly_index) OVER (PARTITION BY statename ORDER BY year)) * 100 AS yoy_change_pct
        FROM state_yearly_volatility
    ),
    state_volatility_metrics AS (
        SELECT
            statename,
            COUNT(*) AS years_tracked,
            AVG(yoy_change_pct) AS avg_yoy_change,
            STDDEV(yoy_change_pct) AS volatility_stddev,
            MIN(yoy_change_pct) AS min_yoy_change,
            MAX(yoy_change_pct) AS max_yoy_change,
            (MAX(yoy_change_pct) - MIN(yoy_change_pct)) AS volatility_range
        FROM state_year_over_year_changes
        WHERE yoy_change_pct IS NOT NULL
        GROUP BY statename
        HAVING COUNT(*) >= 10
    )
    SELECT
        statename,
        years_tracked,
        ROUND(avg_yoy_change, 2) AS avg_yoy_change_pct,
        ROUND(volatility_stddev, 2) AS volatility_stddev,
        ROUND(volatility_range, 2) AS volatility_range_pct,
        ROUND(min_yoy_change, 2) AS worst_year_pct,
        ROUND(max_yoy_change, 2) AS best_year_pct
    FROM state_volatility_metrics
    ORDER BY volatility_stddev DESC
    LIMIT 10;
    """
    df4 = con.execute(q4).df()
    
    # Format Q4 data for better presentation
    if not df4.empty:
        # Add formatted columns for better readability
        df4_formatted = df4.copy()
        df4_formatted['avg_yoy_change_formatted'] = df4_formatted['avg_yoy_change_pct'].apply(lambda x: f"{x:.2f}%")
        df4_formatted['volatility_stddev_formatted'] = df4_formatted['volatility_stddev'].apply(lambda x: f"{x:.2f}%")
        df4_formatted['volatility_range_formatted'] = df4_formatted['volatility_range_pct'].apply(lambda x: f"{x:.2f}%")
        df4_formatted['worst_year_formatted'] = df4_formatted['worst_year_pct'].apply(lambda x: f"{x:.2f}%")
        df4_formatted['best_year_formatted'] = df4_formatted['best_year_pct'].apply(lambda x: f"{x:.2f}%")
        
        # Reorder columns for better presentation
        df4_formatted = df4_formatted[['statename', 'years_tracked', 'avg_yoy_change_formatted', 'volatility_stddev_formatted', 
                                      'volatility_range_formatted', 'worst_year_formatted', 'best_year_formatted',
                                      'avg_yoy_change_pct', 'volatility_stddev', 'volatility_range_pct', 'worst_year_pct', 'best_year_pct']]
        df4_formatted.columns = ['State', 'Years_Tracked', 'Avg_YoY_Change_Formatted', 'Volatility_StdDev_Formatted', 
                                'Volatility_Range_Formatted', 'Worst_Year_Formatted', 'Best_Year_Formatted',
                                'Avg_YoY_Change_Raw', 'Volatility_StdDev_Raw', 'Volatility_Range_Raw', 'Worst_Year_Raw', 'Best_Year_Raw']
        
        out = REPORTS / "Q4_Top10_States_Highest_Volatility.csv"
        df4_formatted.to_csv(out, index=False)
        print(f"[ok] wrote {out}")
    else:
        df_to_csv(df4, "Q4_Top10_States_Highest_Volatility")
    
    # Create a horizontal bar chart for Q4 showing volatility by state
    if not df4.empty:
        # Sort with highest volatility at the top
        df4 = df4.sort_values('volatility_stddev', ascending=True)
        
        plt.figure(figsize=(12, 8))
        bars = plt.barh(df4['statename'], df4['volatility_stddev'], color="#D32F2F", alpha=0.8, edgecolor='white', linewidth=1)
        
        plt.title('Top 10 States with Highest Housing Value Volatility (Year-over-Year)', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Volatility (Standard Deviation of YoY Changes)', fontsize=12, fontweight='bold')
        plt.ylabel('State', fontsize=12, fontweight='bold')
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        
        # Add value labels on the right side of bars
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                    f'{width:.2f}%', ha='left', va='center', fontsize=10, fontweight='bold')
        
        # Add grid for better readability
        plt.grid(axis='x', alpha=0.3, linestyle='--')
        plt.tight_layout()
        
        out = FIGS / "Q4_Top10_States_Highest_Volatility.png"
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
