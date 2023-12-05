CREATE TABLE if not exists superset.taxi_stage (
	year_pickup text NULL,
	month_pickup text NULL,
	day_pickup text NULL,
	hour_pickup text NULL,
	date_pickup date NULL,
	year_dropoff text NULL,
	month_dropoff text NULL,
	day_dropoff text NULL,
	hour_dropoff text NULL,
	date_dropoff date NULL,
	passenger_count int4 NULL,
	trip_distance numeric(18, 2) NULL,
	tip_amount numeric(18, 2) NULL,
	total_amount numeric(18, 2) NULL,
	"event" int4 NOT NULL,
	range_miles text NULL,
	range_tips text NULL,
	taxi_type text NOT NULL
);

truncate table superset.taxi_stage;