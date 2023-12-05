insert into superset.fact_taxi
select ts.* from superset.taxi_stage ts
left join superset.fact_taxi ft on (
	ft.date_pickup = ts.date_pickup
	and ft.range_miles = ts.range_miles
	and ft.range_tips = ts.range_tips
	)
where ft.date_pickup is null;