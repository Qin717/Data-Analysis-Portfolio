#!/usr/bin/env python3
"""
Create a pivot-style CSV for Q1 that matches the Excel format.
This will have years as rows and top 10 states as columns.
"""
import pandas as pd
from pathlib import Path

# Paths
REPO = Path(__file__).resolve().parents[1]
CSV_IN = REPO / "reports" / "Q1_Top10_States_Average_Values.csv"
CSV_OUT = REPO / "reports" / "Q1_Top10_States_Average_Values_Pivot.csv"

def create_pivot_csv():
    # Read the current CSV
    df = pd.read_csv(CSV_IN)
    
    # Get top 10 states by average value (same logic as the chart)
    state_avg = df.groupby('statename')['avg_yearly_index'].mean().sort_values(ascending=False)
    top_10_states = state_avg.head(10).index.tolist()
    
    # Filter to top 10 states only
    df_top10 = df[df['statename'].isin(top_10_states)]
    
    # Create pivot table: years as rows, states as columns
    pivot_df = df_top10.pivot(index='year', columns='statename', values='avg_yearly_index')
    
    # Reorder columns to match the top 10 order
    pivot_df = pivot_df[top_10_states]
    
    # Round to 2 decimal places
    pivot_df = pivot_df.round(2)
    
    # Save the pivot CSV
    pivot_df.to_csv(CSV_OUT)
    print(f"[ok] Created pivot CSV: {CSV_OUT}")
    print(f"[info] Top 10 states: {', '.join(top_10_states)}")
    print(f"[info] Years: {pivot_df.index.min()} to {pivot_df.index.max()}")
    print(f"[info] Shape: {pivot_df.shape}")

if __name__ == "__main__":
    create_pivot_csv()
