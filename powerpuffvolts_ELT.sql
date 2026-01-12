

CREATE SCHEMA IF NOT EXISTS "EV_vehicle"
    AUTHORIZATION postgres;

-- SCHEMA: EV_vehicle
-- Corrected Schema Based on Actual CSV Columns


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



-- STEP 0: CLEAN OLD DATA

TRUNCATE TABLE 
    "EV_vehicle".fact_charging_sessions,
    "EV_vehicle".dim_user,
    "EV_vehicle".dim_vehicle,
    "EV_vehicle".dim_station,
    "EV_vehicle".dim_time,
    "EV_vehicle".dim_environment
RESTART IDENTITY CASCADE;


-- STEP 1: MINIMAL PRE-LOAD CLEANING
-- Remove duplicates & obvious invalid rows to match ETL output

DELETE FROM "EV_vehicle".staging_ev_raw a
USING "EV_vehicle".staging_ev_raw b
WHERE a.ctid < b.ctid
  AND a.user_id = b.user_id
  AND a.start_time = b.start_time
  AND a.station_id = b.station_id;

DELETE FROM "EV_vehicle".staging_ev_raw
WHERE energy_consumed_kwh < 0
   OR charging_cost_usd < 0
   OR distance_driven_km < 0
   OR soc_start_pct < 0 OR soc_start_pct > 100
   OR soc_end_pct < 0 OR soc_end_pct > 100
   OR battery_capacity_kwh <= 0
   OR vehicle_age_years < 0
   OR start_time IS NULL
   OR end_time IS NULL
   OR user_id IS NULL
   OR user_type IS NULL
   OR vehicle_model IS NULL
   OR station_id IS NULL
   OR location_city IS NULL;

-- STEP 2: LOAD DIMENSIONS

INSERT INTO "EV_vehicle".dim_user (user_id, user_type)
SELECT DISTINCT user_id, INITCAP(TRIM(user_type))
FROM "EV_vehicle".staging_ev_raw;

INSERT INTO "EV_vehicle".dim_vehicle (vehicle_model, battery_capacity_kwh, vehicle_age_years)
SELECT DISTINCT INITCAP(TRIM(vehicle_model)), battery_capacity_kwh, vehicle_age_years
FROM "EV_vehicle".staging_ev_raw;

INSERT INTO "EV_vehicle".dim_station (station_id, location_city, charger_type)
SELECT DISTINCT station_id, INITCAP(TRIM(location_city)), INITCAP(TRIM(charger_type))
FROM "EV_vehicle".staging_ev_raw;

INSERT INTO "EV_vehicle".dim_time (start_time, end_time, date, day_of_week, month, year, time_of_day)
SELECT DISTINCT 
    start_time, 
    end_time, 
    DATE(start_time), 
    INITCAP(TRIM(day_of_week)), 
    EXTRACT(MONTH FROM start_time)::INT, 
    EXTRACT(YEAR FROM start_time)::INT,
    INITCAP(TRIM(time_of_day))
FROM "EV_vehicle".staging_ev_raw;

INSERT INTO "EV_vehicle".dim_environment (temperature_c)
SELECT DISTINCT temperature_c
FROM "EV_vehicle".staging_ev_raw;

-- STEP 3: LOAD FACT TABLE

INSERT INTO "EV_vehicle".fact_charging_sessions (
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
    st.energy_consumed_kwh,
    st.charging_cost_usd,
    ROUND(st.charging_duration_hr * 60)::INT AS charging_duration_min,
    st.distance_driven_km,
    st.soc_start_pct,
    st.soc_end_pct
FROM "EV_vehicle".staging_ev_raw st
JOIN "EV_vehicle".dim_user u 
       ON st.user_id = u.user_id AND INITCAP(TRIM(st.user_type)) = u.user_type
JOIN "EV_vehicle".dim_vehicle v 
       ON INITCAP(TRIM(st.vehicle_model)) = v.vehicle_model
      AND st.battery_capacity_kwh = v.battery_capacity_kwh
      AND st.vehicle_age_years = v.vehicle_age_years
JOIN "EV_vehicle".dim_station s 
       ON st.station_id = s.station_id AND INITCAP(TRIM(st.location_city)) = s.location_city
JOIN "EV_vehicle".dim_time t 
       ON st.start_time = t.start_time AND st.end_time = t.end_time
JOIN "EV_vehicle".dim_environment e 
       ON st.temperature_c = e.temperature_c;

-- STEP 4: POST-LOAD TRANSFORMATIONS


-- Fix battery capacity (if any invalid values sneaked in)
UPDATE "EV_vehicle".dim_vehicle
SET battery_capacity_kwh = (
    SELECT AVG(battery_capacity_kwh) FROM "EV_vehicle".dim_vehicle
)
WHERE battery_capacity_kwh <= 0 OR battery_capacity_kwh IS NULL;

-- Recalculate time_of_day
UPDATE "EV_vehicle".dim_time
SET time_of_day = CASE
    WHEN EXTRACT(HOUR FROM start_time) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM start_time) BETWEEN 12 AND 17 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM start_time) BETWEEN 18 AND 23 THEN 'Evening'
    ELSE 'Night'
END;

-- Calculate derived column: charging_efficiency
UPDATE "EV_vehicle".fact_charging_sessions
SET charging_efficiency = CASE 
    WHEN distance_driven_km > 0 THEN energy_consumed_kwh / distance_driven_km
    ELSE NULL
END;

-- Remove outliers
DELETE FROM "EV_vehicle".fact_charging_sessions
WHERE charging_duration_min > 1000
   OR energy_consumed_kwh > 500
   OR soc_end_pct > 100
   OR soc_start_pct < 0;


-- STEP 5: VALIDATION

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