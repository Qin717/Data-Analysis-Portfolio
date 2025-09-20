-- Q1. Calculate the yearly average home value index for each state

SELECT
    statename,
    year,
    ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
FROM home_values_yearly_clean
WHERE yearlyindex IS NOT NULL
GROUP BY statename, year
ORDER BY statename, year;

-- Q2. Which 5 states have shown the highest growth in home values index from 2000 to 2025?

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

-- Q3A. Which top 5 cities have shown the highest ABSOLUTE growth in home value index from 2000 to 2025?

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
    (c2025.value_2025 - c2000.value_2000) AS absolute_growth,
    ROUND(((c2025.value_2025 - c2000.value_2000) / c2000.value_2000) * 100, 2) AS pct_growth
FROM city_2000 c2000
JOIN city_2025 c2025 ON c2000.city = c2025.city AND c2000.statename = c2025.statename
ORDER BY absolute_growth DESC
LIMIT 5;

-- Q3B. Which top 5 cities have shown the highest PERCENTAGE growth in home value index from 2000 to 2025?

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
    (c2025.value_2025 - c2000.value_2000) AS absolute_growth,
    ROUND(((c2025.value_2025 - c2000.value_2000) / c2000.value_2000) * 100, 2) AS pct_growth
FROM city_2000 c2000
JOIN city_2025 c2025 ON c2000.city = c2025.city AND c2000.statename = c2025.statename
ORDER BY pct_growth DESC
LIMIT 5;

-- Q4. Which states show the highest volatility in housing values year-over-year?

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
    HAVING COUNT(*) >= 10  -- Only include states with at least 10 years of data
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

