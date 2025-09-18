-- Q1. Which states have experienced the fastest and slowest growth 
-- in housing values since 2000?

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
ORDER BY pct_growth DESC
LIMIT 5;  -- top 5 fastest growing states

-- To find the slowest, rerun with ORDER BY pct_growth ASC LIMIT 5

-- Q2. Has the gap between the most expensive and least expensive states 
-- widened from 2000 to 2025?

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

