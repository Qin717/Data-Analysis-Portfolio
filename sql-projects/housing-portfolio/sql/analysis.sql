-- Assumes a DuckDB table named home_values_yearly_clean with columns:
--   statename (TEXT), year (INTEGER), yearlyindex (DOUBLE)
-- Case-insensitive identifiers are okay if column case differs.

-- 0) Create table from CSV (run once if using DuckDB CLI or Python)
-- CREATE OR REPLACE TABLE home_values_yearly_clean AS
-- SELECT * FROM read_csv_auto('data/home_values_yearly_clean.csv', header=true);

-- Build an annual average price index per state (one row per state-year)
WITH state_year AS ( -- CTE 1: compute avg index by state and year
  SELECT
    statename,
    year,
    AVG(yearlyindex) AS avg_index
  FROM home_values_yearly_clean
  WHERE yearlyindex IS NOT NULL
  GROUP BY 1,2
),
-- Pivot to get each state's index in 2000 and 2025 on the same row
pivot AS ( -- CTE 2: pivot years 2000 and 2025 into columns
  SELECT
    statename,
    MAX(CASE WHEN year = 2000 THEN avg_index END) AS idx_2000,
    MAX(CASE WHEN year = 2025 THEN avg_index END) AS idx_2025
  FROM state_year
  GROUP BY 1
)
-- Final: compute percent growth from 2000 to 2025 and rank by growth
SELECT
  statename,
  ROUND(idx_2000, 2) AS idx_2000,
  ROUND(idx_2025, 2) AS idx_2025,
  ROUND(((idx_2025 - idx_2000) / idx_2000) * 100, 2) AS pct_growth
FROM pivot
WHERE idx_2000 IS NOT NULL AND idx_2025 IS NOT NULL
ORDER BY pct_growth DESC;

-- Build average index for the two crisis endpoints (2007 and 2009)
WITH state_year AS ( -- CTE 1: avg index by state for years 2007 and 2009
  SELECT statename, year, AVG(yearlyindex) AS avg_index
  FROM home_values_yearly_clean
  WHERE yearlyindex IS NOT NULL AND year IN (2007, 2009)
  GROUP BY 1,2
),
-- Pivot 2007 and 2009 to compute the two points per state
pivot AS ( -- CTE 2: pivot years 2007 and 2009 into columns
  SELECT
    statename,
    MAX(CASE WHEN year = 2007 THEN avg_index END) AS idx_2007,
    MAX(CASE WHEN year = 2009 THEN avg_index END) AS idx_2009
  FROM state_year
  GROUP BY 1
)
-- Final: percent change from 2007 to 2009 (most negative = hardest hit)
SELECT
  statename,
  ROUND(((idx_2009 - idx_2007) / idx_2007) * 100, 2) AS pct_change_07_09
FROM pivot
WHERE idx_2007 IS NOT NULL AND idx_2009 IS NOT NULL
ORDER BY pct_change_07_09 ASC;

-- Build an annual average index per state (baseline for YoY calculations)
WITH state_year AS ( -- CTE 1: avg index by state and year
  SELECT statename, year, AVG(yearlyindex) AS avg_index
  FROM home_values_yearly_clean
  WHERE yearlyindex IS NOT NULL
  GROUP BY 1,2
),
-- Compute year-over-year percent changes for each state and year
yoy AS ( -- CTE 2: YoY % change using previous year's avg_index
  SELECT
    curr.statename,
    curr.year,
    (curr.avg_index - prev.avg_index) / NULLIF(prev.avg_index, 0) AS yoy_pct
  FROM state_year curr
  JOIN state_year prev
    ON curr.statename = prev.statename AND curr.year = prev.year + 1
  WHERE prev.avg_index IS NOT NULL
)
-- Final: volatility = sample standard deviation of YoY % changes, by state
SELECT
  statename,
  ROUND(STDDEV_SAMP(yoy_pct) * 100, 2) AS yoy_volatility_pct
FROM yoy
GROUP BY 1
ORDER BY yoy_volatility_pct DESC;

-- Build average index per state-year to measure dispersion across states each year
WITH state_year AS ( -- CTE 1: avg index by state and year
  SELECT statename, year, AVG(yearlyindex) AS avg_index
  FROM home_values_yearly_clean
  WHERE yearlyindex IS NOT NULL
  GROUP BY 1,2
),
-- For each year, compute the gap between the highest and lowest state averages
gap_by_year AS ( -- CTE 2: yearly max-min gap across states
  SELECT
    year,
    MAX(avg_index) - MIN(avg_index) AS gap
  FROM state_year
  GROUP BY year
)
-- Final: show the gap in the two comparison years (2000 vs 2025)
SELECT
  year,
  ROUND(gap, 2) AS gap
FROM gap_by_year
WHERE year IN (2000, 2025)
ORDER BY year;

-- 5) Top 10 most expensive states in 2025 (by average yearly index)
-- Build average index per state for the year 2025
WITH state_year_2025 AS ( -- CTE: avg index by state for 2025 only
  SELECT
    statename,
    AVG(yearlyindex) AS avg_index_2025
  FROM home_values_yearly_clean
  WHERE yearlyindex IS NOT NULL AND year = 2025
  GROUP BY 1
)
-- Final: list states by highest 2025 average index and keep top 10
SELECT
  statename,
  ROUND(avg_index_2025, 2) AS avg_index_2025
FROM state_year_2025
ORDER BY avg_index_2025 DESC
LIMIT 10;

-- Optional: single-line verdict
-- SELECT CASE WHEN
--   (SELECT gap FROM gap_by_year WHERE year = 2025) >
--   (SELECT gap FROM gap_by_year WHERE year = 2000)
-- THEN 'Yes, widened' ELSE 'No, narrowed or unchanged' END AS widened;
