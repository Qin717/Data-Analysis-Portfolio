-- Q1. Calculate the yearly average home value index for each state

SELECT
    statename,
    year,
    ROUND(AVG(yearlyindex), 2) AS avg_yearly_index
FROM home_values_yearly_clean
WHERE yearlyindex IS NOT NULL
GROUP BY statename, year
ORDER BY statename, year;


