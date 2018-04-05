-- query to join tables and save to csv
Copy (
Select * From fact_table JOIN date sd ON fact_table.start_date_key = sd.date_key
	JOIN date ed ON fact_table.end_date_key = ed.date_key
	JOIN costs ON fact_table.cost_key = costs.costs_key
	JOIN disaster ON fact_table.disaster_key = disaster.disaster_key
	JOIN location ON fact_table.location_key = location.location_key
	JOIN summary ON fact_table.summary_key = summary.summary_key
) To '/Users/qufeichen/Documents/Repos/CSI4142-Project/7-AnomalyDetection/data.csv' With CSV DELIMITER ',' HEADER;

Copy (
Select fact_table.fatalities, fact_table.injured, fact_table.evacuated,
	costs.estimated_total_cost, costs.normalized_total_cost, costs.federal_payments, costs.provincial_payments, costs.insurance_payments,
	sd.day as start_day, sd.week as start_week, sd.month as start_month, sd.year as start_year, sd.weekend as start_weekend, sd.season_canada as start_season_canada,
	ed.day as end_day, ed.week as end_week, ed.month as end_month, ed.year as end_year, ed.weekend as end_weekend, ed.season_canada as end_season_canada,
	disaster.disaster_type, disaster.disaster_subgroup, disaster.disaster_group, disaster.disaster_category, disaster.magnitude, disaster.utility_people_affected,
	location.city, location.province, location.country, location.canada,
	summary.summary
	From fact_table JOIN date sd ON fact_table.start_date_key = sd.date_key
	JOIN date ed ON fact_table.end_date_key = ed.date_key
	JOIN costs ON fact_table.cost_key = costs.costs_key
	JOIN disaster ON fact_table.disaster_key = disaster.disaster_key
	JOIN location ON fact_table.location_key = location.location_key
	JOIN summary ON fact_table.summary_key = summary.summary_key
) To '/Users/qufeichen/Documents/Repos/CSI4142-Project/7-AnomalyDetection/data.csv' With CSV DELIMITER ',' HEADER;
-- note: need to remove all "" (back to back double quotes) from the resulting csv before importing into weka (or else there will be an error)
