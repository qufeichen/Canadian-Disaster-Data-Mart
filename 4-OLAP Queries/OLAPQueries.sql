-- Total number of injuries in Canada in 1996 by province.
-- NOTE: not sure if I should just filter by start date? Or by end date?

SELECT l.Province, sum(f.injuries)
	FROM Location l, Fact f, Date d1, Date d2
	WHERE l.Canada = TRUE AND l.Province = “ON” AND f.Location-Key = l.Location-Key
	AND d1.Year = 1996 AND d2.Year = 1996 AND f.Start-Date-key = d1.Date-key, f.End-Date-key = d2.Date-key
	GROUP BY l.Province


-- Years with the most deaths
SELECT TOP 10 d1.Year, sum(f.fatalities) as deaths
	FROM Fact f, Date d1, Date d2
	WHERE d1.Year = d2.Year, f.Start-Date-key = d1.Date-key, f.End-Date-key = d2.Date-key
	GROUP BY d1.year
	ORDER BY deaths DESC


-- Comparison of fatalities by disaster type in Ontario, in 2000
SELECT l.Province,  d.Disaster-type, sum(f.fatalities)
	FROM Location l, Fact f, Disaster d, Date d1, Date d2
	WHERE l.Canada = TRUE AND l.Province = “ON” AND f.Location-Key = l.Location-Key AND f.Disaster-key = d.Disaster-key
	AND d1.Year = 2000 AND d2.Year = 2000 AND f.Start-Date-key = d1.Date-key AND f.End-Date-key = d2.Date-key
	GROUP BY d.Disaster-type


-- Cities in Canada with the most fires
SELECT top 10 l.City,  d.Disaster-type, sum(f) as num
	FROM Location l, Fact f, Disaster d,
	WHERE l.Canada = TRUE, d.Disaster-type LIKE “%fire%”
	f.Location-Key = l.Location-Key, f.Disaster-key = d.Disaster-key,
	GROUP BY l.City
	ORDER BY num DESC

-- Drill down on number of disasters of each group, and subgroup.
SELECT d.group, sum(f)
	FROM Disaster d INNER JOIN Fact f ON d.Disaster-key = f.Disaster-key
	GROUP BY d.group

SELECT d.subgroup, d.group, sum(f)
	FROM Disaster d INNER JOIN Fact f ON d.Disaster-key = f.Disaster-key
	GROUP BY d.subgroup

-- Or Drill down on average price of disasters per province, and per city
SELECT l.Province, avg(c.estimated-cost), avg(c.normalized-cost)
	FROM Location l INNER JOIN Fact f ON l.Location-key = f.Location-key
	INNER JOIN Cost c ON f.Cost-key = c.Cost-key
	GROUP BY l.Province

SELECT l.City, avg(c.estimated-cost), avg(c.normalized-cost)
	FROM Location l INNER JOIN Fact f ON l.Location-key = f.Location-key
	INNER JOIN Cost c ON f.Cost-key = c.Cost-key
	GROUP BY l.City