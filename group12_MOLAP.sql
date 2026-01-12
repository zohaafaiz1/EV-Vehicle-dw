-- SCHEMA: EV_vehicle

-- DROP SCHEMA IF EXISTS "EV_vehicle" ;

CREATE SCHEMA IF NOT EXISTS "EV_vehicle"
    AUTHORIZATION postgres;
	

-- 2) molap: create materialized views (precomputed aggregates)
--      we create 3 example molap objects:
--        - monthly_city_summary
--        - monthly_user_type_summary
--        - station_monthly_summary


set search_path = "ev_vehicle", public;

-- a. monthly energy & sessions by city (molap)
drop materialized view if exists mv_monthly_city_summary cascade;
create materialized view mv_monthly_city_summary as
select
  t.year,
  t.month,
  s.location_city,
  sum(f.energy_consumed_kwh)::numeric(18,2) as total_energy_kwh,
  sum(f.charging_cost_usd)::numeric(18,2) as total_cost_usd,
  count(*) as sessions_count
from fact_charging_sessions f
join dim_time t    on f.time_key = t.time_key
join dim_station s on f.station_key = s.station_key
group by t.year, t.month, s.location_city
with data;

-- add indexes (and a unique index to support concurrently refresh if needed)
create index if not exists mv_month_city_year_month_idx
  on mv_monthly_city_summary (year, month);
create index if not exists mv_month_city_city_idx
  on mv_monthly_city_summary (location_city);

do $$
begin
  begin
    create unique index mv_month_city_unique on mv_monthly_city_summary (year, month, location_city);
  exception when others then
    raise notice 'could not create unique index mv_month_city_unique (possibly duplicates exist).';
  end;
end$$;

-- b. monthly aggregates by user type (molap)
drop materialized view if exists mv_monthly_user_type_summary cascade;
create materialized view mv_monthly_user_type_summary as
select
  t.year,
  t.month,
  u.user_type,
  count(*) as sessions_count,
  round(avg(f.charging_cost_usd)::numeric,2) as avg_cost_usd,
  sum(f.energy_consumed_kwh)::numeric(18,2) as total_energy_kwh
from fact_charging_sessions f
join dim_time t on f.time_key = t.time_key
join dim_user u on f.user_key = u.user_key
group by t.year, t.month, u.user_type
with data;

create index if not exists mv_month_user_year_month_idx
  on mv_monthly_user_type_summary (year, month);
create index if not exists mv_month_user_type_idx
  on mv_monthly_user_type_summary (user_type);

do $$
begin
  begin
    create unique index mv_month_user_type_unique on mv_monthly_user_type_summary (year, month, user_type);
  exception when others then
    raise notice 'could not create unique index mv_month_user_type_unique (possibly duplicates exist).';
  end;
end$$;

-- c. station monthly summary (molap)
drop materialized view if exists mv_station_monthly_summary cascade;
create materialized view mv_station_monthly_summary as
select
  t.year,
  t.month,
  s.station_id,
  s.location_city,
  s.charger_type,
  count(*) as sessions_count,
  sum(f.energy_consumed_kwh)::numeric(18,2) as total_energy_kwh,
  sum(f.charging_cost_usd)::numeric(18,2) as total_cost_usd,
  round(avg(f.charging_duration_min)::numeric,2) as avg_duration_min
from fact_charging_sessions f
join dim_time t    on f.time_key = t.time_key
join dim_station s on f.station_key = s.station_key
group by t.year, t.month, s.station_id, s.location_city, s.charger_type
with data;

create index if not exists mv_station_month_idx
  on mv_station_monthly_summary (station_id, year, month);

do $$
begin
  begin
    create unique index mv_station_month_unique on mv_station_monthly_summary (year, month, station_id);
  exception when others then
    raise notice 'could not create unique index mv_station_month_unique (possibly duplicates exist).';
  end;
end$$;

-- d. refresh pattern (blocking refreshes below)
refresh materialized view mv_monthly_city_summary;
refresh materialized view mv_monthly_user_type_summary;
refresh materialized view mv_station_monthly_summary;

-- e. quick selects to show mv contents (first few rows)
select * from mv_monthly_city_summary order by year, month limit 50;
select * from mv_monthly_user_type_summary order by year, month limit 50;
select * from mv_station_monthly_summary order by year, month limit 50;

-- f. verification: compare live (olap) aggregation vs molap (mv_monthly_city_summary)
with live as (
  select
    t.year,
    t.month,
    s.location_city,
    sum(f.energy_consumed_kwh)::numeric(18,2) as total_energy_kwh,
    sum(f.charging_cost_usd)::numeric(18,2) as total_cost_usd,
    count(*) as sessions_count
  from fact_charging_sessions f
  join dim_time t    on f.time_key = t.time_key
  join dim_station s on f.station_key = s.station_key
  group by t.year, t.month, s.location_city
),
mv as (
  select year, month, location_city, total_energy_kwh, total_cost_usd, sessions_count
  from mv_monthly_city_summary
)
select
  coalesce(live.year::text, mv.year::text) as year,
  coalesce(live.month::text, mv.month::text) as month,
  coalesce(live.location_city, mv.location_city) as location_city,
  live.total_energy_kwh as live_total_energy_kwh,
  mv.total_energy_kwh as mv_total_energy_kwh,
  live.total_cost_usd as live_total_cost_usd,
  mv.total_cost_usd as mv_total_cost_usd,
  live.sessions_count as live_sessions_count,
  mv.sessions_count as mv_sessions_count,
  case
    when live.year is null then 'in_mv_not_in_live'
    when mv.year is null then 'in_live_not_in_mv'
    when (live.total_energy_kwh is distinct from mv.total_energy_kwh)
      or (live.total_cost_usd is distinct from mv.total_cost_usd)
      or (live.sessions_count is distinct from mv.sessions_count) then 'value_mismatch'
    else 'match'
  end as comparison_status
from live
full outer join mv using (year, month, location_city)
order by year, month, location_city
limit 200;

select count(*) from "ev_vehicle".dim_user;
select count(*) from "ev_vehicle".dim_vehicle;
select count(*) from "ev_vehicle".dim_station;
select count(*) from "ev_vehicle".dim_time;
select count(*) from "ev_vehicle".dim_environment;
select count(*) from "ev_vehicle".fact_charging_sessions;
