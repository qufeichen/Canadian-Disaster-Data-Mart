-- Total number of injuries and fatalities in Canada in 1996 by province.

SELECT l.Province, sum(f.injured), sum(f.fatalities)
	FROM Location l, Fact_Table f, Date d1, Date d2
	WHERE l.Canada = TRUE AND l.Province = 'ON' AND f.Location_Key = l.Location_Key
	AND d1.Year = 1996 AND d2.Year = 1996 AND f.Start_Date_key = d1.Date_key AND f.End_Date_key = d2.Date_key
	GROUP BY l.Province


-- Years with the most deaths
SELECT d1.Year, sum(f.fatalities) as deaths
	FROM Fact_Table f, Date d1, Date d2
	WHERE d1.Year = d2.Year AND f.Start_Date_key = d1.Date_key AND f.End_Date_key = d2.Date_key
	GROUP BY d1.year
	ORDER BY deaths DESC
	LIMIT 10


-- Comparison of fatalities by disaster type in British Columbia, in from the years 2000 to 2010
SELECT d.Disaster_type, sum(f.fatalities)
	FROM Location l, Fact_Table f, Disaster d, Date d1, Date d2
	WHERE l.Canada = TRUE AND l.Province = 'BC' AND f.Location_Key = l.Location_Key AND f.Disaster_key = d.Disaster_key
	AND d1.Year >= 2000 AND d2.Year <= 2010 AND f.Start_Date_key = d1.Date_key AND f.End_Date_key = d2.Date_key
	GROUP BY d.Disaster_type


-- Cities in Canada with the most fires
SELECT l.City, count(f) as num
	FROM Location l, Fact_Table f, Disaster d
	WHERE l.Canada = TRUE AND d.Disaster_Subgroup LIKE '%Fire%'
	AND f.Location_Key = l.Location_Key AND f.Disaster_key = d.Disaster_key
	GROUP BY l.City
	ORDER BY num DESC
	LIMIT 10

-- Drill down on number of disasters of each group, and subgroup.
SELECT d.disaster_group, count(f)
	FROM Disaster d INNER JOIN Fact_Table f ON d.Disaster_key = f.Disaster_key
	GROUP BY d.disaster_group

SELECT d.disaster_subgroup, d.disaster_group, count(f)
	FROM Disaster d INNER JOIN Fact_Table f ON d.Disaster_key = f.Disaster_key
	GROUP BY d.disaster_subgroup

-- Drill down on average price of disasters per province, and per city
SELECT l.Province, avg(c.estimated_total_cost), avg(c.normalized_total_cost)
	FROM Location l INNER JOIN Fact_Table f ON l.Location_key = f.Location_key
	INNER JOIN Costs c ON f.Cost_key = c.Costs_key
	GROUP BY l.Province

SELECT l.City, avg(c.estimated_total_cost), avg(c.normalized_total_cost)
	FROM Location l INNER JOIN Fact_Table f ON l.Location_key = f.Location_key
	INNER JOIN Costs c ON f.Cost_key = c.Costs_key
	GROUP BY l.City


