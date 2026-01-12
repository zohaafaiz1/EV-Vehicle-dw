CREATE SCHEMA IF NOT EXISTS "EV_vehicle"
    AUTHORIZATION postgres;

-- 1️⃣ USER DIMENSION
CREATE TABLE "EV_vehicle".dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    user_type VARCHAR(30)
);

-- 2️⃣ VEHICLE DIMENSION
CREATE TABLE "EV_vehicle".dim_vehicle (
    vehicle_key SERIAL PRIMARY KEY,
    vehicle_model VARCHAR(100),
    battery_capacity_kwh DECIMAL(6,2),
    vehicle_age_years INT
);

ALTER TABLE "EV_vehicle".dim_vehicle
ALTER COLUMN vehicle_age_years TYPE DECIMAL(5,2)
USING vehicle_age_years::DECIMAL(5,2);

-- 3️⃣ STATION DIMENSION
CREATE TABLE "EV_vehicle".dim_station (
    station_key SERIAL PRIMARY KEY,
    station_id VARCHAR(50),
    location_city VARCHAR(100),
    charger_type VARCHAR(50)
);

-- 4️⃣ TIME DIMENSION
CREATE TABLE "EV_vehicle".dim_time (
    time_key SERIAL PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    date DATE,
    day_of_week VARCHAR(15),
    month INT,
    year INT,
    time_of_day VARCHAR(20)
);
-- 5️⃣ ENVIRONMENT DIMENSION (Simplified)
CREATE TABLE "EV_vehicle".dim_environment (
    environment_key SERIAL PRIMARY KEY,
    temperature_c DECIMAL(5,2)
);


-- 6️⃣ FACT TABLE - FINAL (dataset accurate)
CREATE TABLE "EV_vehicle".fact_charging_sessions (
    session_key SERIAL PRIMARY KEY,

    -- Foreign Keys
    user_key INT REFERENCES "EV_vehicle".dim_user(user_key),
    vehicle_key INT REFERENCES "EV_vehicle".dim_vehicle(vehicle_key),
    station_key INT REFERENCES "EV_vehicle".dim_station(station_key),
    time_key INT REFERENCES "EV_vehicle".dim_time(time_key),
    environment_key INT REFERENCES "EV_vehicle".dim_environment(environment_key),

    -- Measures (all numeric / analytical fields)
    energy_consumed_kwh DECIMAL(8,2),
    charging_cost_usd DECIMAL(8,2),
    charging_duration_min INT,
    distance_driven_km DECIMAL(8,2),
    soc_start_pct DECIMAL(5,2),
    soc_end_pct DECIMAL(5,2)
);




DROP TABLE IF EXISTS "EV_vehicle".staging_ev_raw CASCADE;

CREATE TABLE "EV_vehicle".staging_ev_raw (
    session_id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    vehicle_model VARCHAR(100),
    battery_capacity_kwh DECIMAL(6,2),
    station_id VARCHAR(50),
    location_city VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    energy_consumed_kwh DECIMAL(8,2),
    charging_duration_hr DECIMAL(5,2),
    charging_rate_kw DECIMAL(6,2),
    charging_cost_usd DECIMAL(8,2),
    time_of_day VARCHAR(20),
    day_of_week VARCHAR(15),
    soc_start_pct DECIMAL(5,2),
    soc_end_pct DECIMAL(5,2),
    distance_driven_km DECIMAL(8,2),
    temperature_c DECIMAL(5,2),
    vehicle_age_years DECIMAL(5,2),  -- ✅ changed from INT
    charger_type VARCHAR(50),
    user_type VARCHAR(30)
);

-- 1️⃣ TRUNCATE PREVIOUS DATA
TRUNCATE TABLE "EV_vehicle".fact_charging_sessions RESTART IDENTITY CASCADE;
TRUNCATE TABLE "EV_vehicle".dim_user RESTART IDENTITY CASCADE;
TRUNCATE TABLE "EV_vehicle".dim_vehicle RESTART IDENTITY CASCADE;
TRUNCATE TABLE "EV_vehicle".dim_station RESTART IDENTITY CASCADE;
TRUNCATE TABLE "EV_vehicle".dim_time RESTART IDENTITY CASCADE;
TRUNCATE TABLE "EV_vehicle".dim_environment RESTART IDENTITY CASCADE;

-- 2️⃣ TRANSFORMATIONS ON STAGING

-- 🔹 Transformation 1: Remove exact duplicates (same user + start_time + station)
DELETE FROM "EV_vehicle".staging_ev_raw a
USING "EV_vehicle".staging_ev_raw b
WHERE a.ctid < b.ctid
  AND a.user_id = b.user_id
  AND a.start_time = b.start_time
  AND a.station_id = b.station_id;

-- 🔹 Transformation 2: Currency normalization & fill NULLs
UPDATE "EV_vehicle".staging_ev_raw
SET charging_cost_usd = COALESCE(
    CASE 
        WHEN charging_cost_usd > 1000 THEN charging_cost_usd / 100  
        ELSE charging_cost_usd
    END,
    0
);

-- 🔹 Transformation 3 : Fill other numeric NULLs
UPDATE "EV_vehicle".staging_ev_raw
SET 
    energy_consumed_kwh = COALESCE(energy_consumed_kwh, 0),
    distance_driven_km  = COALESCE(distance_driven_km, 0),
    battery_capacity_kwh= COALESCE(battery_capacity_kwh, 0),
    vehicle_age_years   = COALESCE(vehicle_age_years, 0),
    soc_start_pct       = COALESCE(soc_start_pct, 0),
    soc_end_pct         = COALESCE(soc_end_pct, 0),
    temperature_c       = COALESCE(temperature_c, 
                            (SELECT ROUND(AVG(temperature_c),2) 
                             FROM "EV_vehicle".staging_ev_raw 
                             WHERE temperature_c IS NOT NULL));

-- 🔹 Transformation 4: Standardize strings
UPDATE "EV_vehicle".staging_ev_raw
SET 
    location_city = INITCAP(TRIM(location_city)),
    vehicle_model = INITCAP(TRIM(vehicle_model)),
    user_type     = INITCAP(TRIM(user_type)),
    charger_type  = INITCAP(TRIM(charger_type)),
    day_of_week   = INITCAP(TRIM(day_of_week)),
    time_of_day   = INITCAP(TRIM(time_of_day));

-- 🔹 Remove invalid/nonsensical values
DELETE FROM "EV_vehicle".staging_ev_raw
WHERE energy_consumed_kwh < 0
   OR charging_cost_usd < 0
   OR distance_driven_km < 0
   OR soc_start_pct < 0 OR soc_start_pct > 100
   OR soc_end_pct < 0 OR soc_end_pct > 100
   OR battery_capacity_kwh <= 0
   OR vehicle_age_years < 0;

-- Optional sanity check
SELECT COUNT(*) AS cleaned_records FROM "EV_vehicle".staging_ev_raw;

-- 3️⃣ LOAD DIMENSION TABLES

-- USER DIM
INSERT INTO "EV_vehicle".dim_user (user_id, user_type)
SELECT DISTINCT user_id, user_type
FROM "EV_vehicle".staging_ev_raw
WHERE user_id IS NOT NULL
  AND user_type IS NOT NULL;

-- VEHICLE DIM
INSERT INTO "EV_vehicle".dim_vehicle (vehicle_model, battery_capacity_kwh, vehicle_age_years)
SELECT DISTINCT vehicle_model, battery_capacity_kwh, vehicle_age_years
FROM "EV_vehicle".staging_ev_raw
WHERE vehicle_model IS NOT NULL
  AND battery_capacity_kwh > 0
  AND vehicle_age_years >= 0;

-- STATION DIM
INSERT INTO "EV_vehicle".dim_station (station_id, location_city, charger_type)
SELECT DISTINCT station_id, location_city, charger_type
FROM "EV_vehicle".staging_ev_raw
WHERE station_id IS NOT NULL
  AND location_city IS NOT NULL;

-- TIME DIM
INSERT INTO "EV_vehicle".dim_time (start_time, end_time, date, day_of_week, month, year, time_of_day)
SELECT DISTINCT 
    start_time, end_time, DATE(start_time), day_of_week, 
    EXTRACT(MONTH FROM start_time)::INT, 
    EXTRACT(YEAR FROM start_time)::INT,
    time_of_day
FROM "EV_vehicle".staging_ev_raw
WHERE start_time IS NOT NULL
  AND end_time IS NOT NULL;

-- ENVIRONMENT DIM
INSERT INTO "EV_vehicle".dim_environment (temperature_c)
SELECT DISTINCT temperature_c
FROM "EV_vehicle".staging_ev_raw
WHERE temperature_c IS NOT NULL;
INSERT INTO "EV_vehicle".fact_charging_sessions  (
    user_key, vehicle_key, station_key, time_key, environment_key,
    energy_consumed_kwh, charging_cost_usd, charging_duration_min,
    distance_driven_km, soc_start_pct, soc_end_pct
)
SELECT 
    u.user_key,
    v.vehicle_key,
    s.station_key,
    t.time_key,
    e.environment_key,
    -- Measures with safe defaults
    GREATEST(st.energy_consumed_kwh,0),
    COALESCE(
        CASE WHEN st.charging_cost_usd > 1000 THEN st.charging_cost_usd/100 ELSE st.charging_cost_usd END, 0
    ),
    ROUND(COALESCE(st.charging_duration_hr,0) * 60)::INT,
    GREATEST(st.distance_driven_km,0),
    CASE WHEN st.soc_start_pct<0 OR st.soc_start_pct>100 THEN 0 ELSE st.soc_start_pct END,
    CASE WHEN st.soc_end_pct<0 OR st.soc_end_pct>100 THEN 0 ELSE st.soc_end_pct END
FROM "EV_vehicle".staging_ev_raw st
JOIN "EV_vehicle".dim_user u 
       ON st.user_id = u.user_id 
      AND INITCAP(TRIM(st.user_type)) = u.user_type
JOIN "EV_vehicle".dim_vehicle v 
       ON INITCAP(TRIM(st.vehicle_model)) = v.vehicle_model
      AND COALESCE(st.battery_capacity_kwh,0) = v.battery_capacity_kwh
      AND COALESCE(st.vehicle_age_years,0) = v.vehicle_age_years
JOIN "EV_vehicle".dim_station s 
       ON st.station_id = s.station_id 
      AND INITCAP(TRIM(st.location_city)) = s.location_city
JOIN "EV_vehicle".dim_time t 
       ON st.start_time = t.start_time 
      AND st.end_time = t.end_time
JOIN "EV_vehicle".dim_environment e 
       ON COALESCE(st.temperature_c,0) = e.temperature_c;

	   
-- ⿥ VALIDATION CHECK
SELECT 'dim_user' AS table_name, COUNT(*) FROM "EV_vehicle".dim_user
UNION ALL
SELECT 'dim_vehicle', COUNT(*) FROM "EV_vehicle".dim_vehicle
UNION ALL
SELECT 'dim_station', COUNT(*) FROM "EV_vehicle".dim_station
UNION ALL
SELECT 'dim_time', COUNT(*) FROM "EV_vehicle".dim_time
UNION ALL
SELECT 'dim_environment', COUNT(*) FROM "EV_vehicle".dim_environment
UNION ALL
SELECT 'fact_charging_sessions', COUNT(*) FROM "EV_vehicle".fact_charging_sessions;
