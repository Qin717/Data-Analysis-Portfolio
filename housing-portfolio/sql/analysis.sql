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


