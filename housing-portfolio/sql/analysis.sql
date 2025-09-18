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

-- Q3. Which are the top 5 most consistent states that ranked in the top 5 
-- and bottom 5 most consistent states that ranked in the bottom 5 
-- in average housing values across the years (minimum 10 years)?

-- Simple approach: Step by step
-- Step 1: Calculate average index per state per year
WITH state_yearly_avg AS (
    SELECT 
        statename,
        year,
        AVG(yearlyindex) AS avg_index
    FROM home_values_yearly_clean
    GROUP BY statename, year
),

-- Step 2: Rank states each year (1=highest, 2=second highest, etc.)
yearly_ranks AS (
    SELECT 
        statename,
        year,
        avg_index,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_index DESC) AS rank_high,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY avg_index ASC) AS rank_low
    FROM state_yearly_avg
),

-- Step 3: Count years in top 5 and bottom 5
consistency AS (
    SELECT 
        statename,
        COUNT(CASE WHEN rank_high <= 5 THEN 1 END) AS years_in_top5,
        COUNT(CASE WHEN rank_low <= 5 THEN 1 END) AS years_in_bottom5,
        ROUND(AVG(avg_index), 2) AS avg_index_overall
    FROM yearly_ranks
    GROUP BY statename
)

-- Step 4: Get top 5 most consistent high performers
SELECT 'Top 5 Consistent' AS category, statename, years_in_top5 AS years_count, avg_index_overall
FROM consistency 
WHERE years_in_top5 >= 10
ORDER BY years_in_top5 DESC
LIMIT 5

UNION ALL

-- Step 5: Get top 5 most consistent low performers  
SELECT 'Bottom 5 Consistent' AS category, statename, years_in_bottom5 AS years_count, avg_index_overall
FROM consistency 
WHERE years_in_bottom5 >= 10
ORDER BY years_in_bottom5 DESC
LIMIT 5;
