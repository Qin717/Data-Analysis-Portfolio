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

-- Q3. Which top 5 cities have shown the highest growth in home value index from 2000 to 2025?

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

-- Q4. How many cities & counties are in each state?

SELECT
    statename,
    COUNT(DISTINCT city) AS unique_cities,
    COUNT(DISTINCT countyname) AS unique_counties
FROM home_values_yearly_clean
GROUP BY statename
ORDER BY unique_cities DESC;


