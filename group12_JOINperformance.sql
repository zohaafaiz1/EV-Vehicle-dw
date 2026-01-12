-- schema: ev_vehicle

-- drop schema if exists "ev_vehicle" ;

create schema if not exists "ev_vehicle"
    authorization postgres;

-- make sure the join_perf table exists
create temp table if not exists join_perf (
    join_type text,
    tables_joined text,
    execution_time_ms numeric,
    observations text
);

-- clear previous entries
truncate table join_perf;

-- 1️ nested loop join
do $$
declare
    t_start timestamp;
    t_end timestamp;
    exec_ms numeric;
begin
    -- planner settings
    set enable_hashjoin = off;
    set enable_mergejoin = off;
    set enable_nestloop = on;

    t_start := clock_timestamp();

    perform f.session_key, u.user_id, v.vehicle_model
    from "ev_vehicle".fact_charging_sessions f
    join "ev_vehicle".dim_user u on f.user_key = u.user_key
    join "ev_vehicle".dim_vehicle v on f.vehicle_key = v.vehicle_key
    limit 100;

    t_end := clock_timestamp();
    exec_ms := extract(millisecond from t_end - t_start);

    insert into join_perf(join_type, tables_joined, execution_time_ms, observations)
    values ('nested loop', 'fact_charging_sessions + dim_user + dim_vehicle', exec_ms, 'small subset; nested loop');

end$$;

-- 2️ sort-merge join
do $$
declare
    t_start timestamp;
    t_end timestamp;
    exec_ms numeric;
begin
    -- planner settings
    set enable_hashjoin = off;
    set enable_mergejoin = on;
    set enable_nestloop = off;

    t_start := clock_timestamp();

    perform f.session_key, t.date, s.station_id
    from "ev_vehicle".fact_charging_sessions f
    join "ev_vehicle".dim_time t on f.time_key = t.time_key
    join "ev_vehicle".dim_station s on f.station_key = s.station_key
    limit 100;

    t_end := clock_timestamp();
    exec_ms := extract(millisecond from t_end - t_start);

    insert into join_perf(join_type, tables_joined, execution_time_ms, observations)
    values ('sort-merge', 'fact_charging_sessions + dim_time + dim_station', exec_ms, 'small subset; sort-merge');

end$$;

-- 3️ hash join
do $$
declare
    t_start timestamp;
    t_end timestamp;
    exec_ms numeric;
begin
    -- planner settings
    set enable_hashjoin = on;
    set enable_mergejoin = off;
    set enable_nestloop = off;

    t_start := clock_timestamp();

    perform f.session_key, u.user_id, v.vehicle_model
    from "ev_vehicle".fact_charging_sessions f
    join "ev_vehicle".dim_user u on f.user_key = u.user_key
    join "ev_vehicle".dim_vehicle v on f.vehicle_key = v.vehicle_key
    limit 100;

    t_end := clock_timestamp();
    exec_ms := extract(millisecond from t_end - t_start);

    insert into join_perf(join_type, tables_joined, execution_time_ms, observations)
    values ('hash join', 'fact_charging_sessions + dim_user + dim_vehicle', exec_ms, 'small subset; hash join');

end$$;

reset enable_hashjoin;
reset enable_mergejoin;
reset enable_nestloop;
select * from join_perf order by join_type;

-- make sure the query_perf table exists
create temp table if not exists query_perf (
    query_type text,
    query_description text,
    execution_time_ms numeric,
    observations text
);

truncate table query_perf;

-- 1️ dss query
do $$
declare
    t_start timestamp;
    t_end timestamp;
    exec_ms numeric;
begin
    t_start := clock_timestamp();

    perform t.year, t.month, s.location_city, sum(f.charging_cost_usd) as monthly_revenue
    from "ev_vehicle".fact_charging_sessions f
    join "ev_vehicle".dim_time t on f.time_key = t.time_key
    join "ev_vehicle".dim_station s on f.station_key = s.station_key
    group by t.year, t.month, s.location_city
    order by t.year, t.month
    limit 100;

    t_end := clock_timestamp();
    exec_ms := extract(millisecond from t_end - t_start);

    insert into query_perf(query_type, query_description, execution_time_ms, observations)
    values ('dss', 'monthly revenue by city', exec_ms, 'aggregates large fact table; uses indexes if present');

end$$;

-- 2️ oltp query
do $$
declare
    t_start timestamp;
    t_end timestamp;
    exec_ms numeric;
begin
    t_start := clock_timestamp();

    perform * 
    from "ev_vehicle".fact_charging_sessions
    where session_key = 1;

    t_end := clock_timestamp();
    exec_ms := extract(millisecond from t_end - t_start);

    insert into query_perf(query_type, query_description, execution_time_ms, observations)
    values ('oltp', 'find session by id', exec_ms, 'direct index lookup; very fast');

end$$;

-- display dss vs oltp comparison
select * from query_perf order by query_type;
