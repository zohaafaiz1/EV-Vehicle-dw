-- schema: ev_vehicle

-- drop schema if exists "ev_vehicle" ;

create schema if not exists "ev_vehicle"
    authorization postgres;

	/* ======================================================
   olap queries & molap summary tables (postgresql)
   ======================================================

   - olap example queries (on-the-fly aggregates)
   - molap: materialized views (precomputed aggregated data)
   - verification queries to compare molap vs olap results
   - indexes on summary objects and refresh commands
   ====================================================== */


-- ------------------------
-- 1) olap (ad-hoc analytical) queries
-- ------------------------
-- groupx_olap.sql
-- olap (ad-hoc analytical) queries for ev_vehicle schema
-- run this after your etl has populated the dims & fact.

-- a. monthly energy consumption (kwh) by city
select
  t.year,
  t.month,
  s.location_city,
  sum(f.energy_consumed_kwh) as total_energy_kwh,
  count(*) as sessions_count
from "ev_vehicle".fact_charging_sessions f
join "ev_vehicle".dim_time t    on f.time_key = t.time_key
join "ev_vehicle".dim_station s on f.station_key = s.station_key
group by t.year, t.month, s.location_city
order by t.year, t.month, s.location_city;

-- b. monthly average charging cost per user_type
select
  t.year, t.month,
  u.user_type,
  round(avg(f.charging_cost_usd)::numeric,2) as avg_cost_usd,
  sum(f.charging_cost_usd) as total_cost_usd,
  count(*) as sessions_count
from "ev_vehicle".fact_charging_sessions f
join "ev_vehicle".dim_time t    on f.time_key = t.time_key
join "ev_vehicle".dim_user u    on f.user_key = u.user_key
group by t.year, t.month, u.user_type
order by t.year, t.month, u.user_type;

-- c. top 10 stations by total energy consumed (overall)
select
  s.station_id,
  s.location_city,
  sum(f.energy_consumed_kwh) as total_energy_kwh,
  count(*) as sessions_count
from "ev_vehicle".fact_charging_sessions f
join "ev_vehicle".dim_station s on f.station_key = s.station_key
group by s.station_id, s.location_city
order by total_energy_kwh desc
limit 10;

-- d. average energy consumed by vehicle battery capacity bucket (example buckets)
select
  case
    when v.battery_capacity_kwh < 40 then '<40'
    when v.battery_capacity_kwh between 40 and 59.99 then '40-59.99'
    when v.battery_capacity_kwh between 60 and 79.99 then '60-79.99'
    else '>=80'
  end as battery_bucket,
  round(avg(f.energy_consumed_kwh)::numeric,2) as avg_energy_kwh,
  count(*) as sessions_count
from "ev_vehicle".fact_charging_sessions f
join "ev_vehicle".dim_vehicle v on f.vehicle_key = v.vehicle_key
group by battery_bucket
order by battery_bucket;

-- e. effect of temperature on energy: average energy by temperature rounded to 1 degree
select
  round(e.temperature_c, 1) as temp_rounded,
  round(avg(f.energy_consumed_kwh)::numeric,2) as avg_energy_kwh,
  count(*) as sessions_count
from "ev_vehicle".fact_charging_sessions f
join "ev_vehicle".dim_environment e on f.environment_key = e.environment_key
group by temp_rounded
order by temp_rounded;

-- f. peak hours: sessions by time_of_day (from dim_time)
select
  t.time_of_day,
  count(*) as sessions_count,
  round(avg(f.charging_duration_min)::numeric,2) as avg_duration_min,
  round(avg(f.charging_cost_usd)::numeric,2) as avg_cost_usd
from "ev_vehicle".fact_charging_sessions f
join "ev_vehicle".dim_time t on f.time_key = t.time_key
group by t.time_of_day
order by sessions_count desc;

