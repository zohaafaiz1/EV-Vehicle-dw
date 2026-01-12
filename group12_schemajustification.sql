-- SCHEMA: EV_vehicle

-- DROP SCHEMA IF EXISTS "EV_vehicle" ;

CREATE SCHEMA IF NOT EXISTS "EV_vehicle"
    AUTHORIZATION postgres;


/*
SCHEMA IMPLEMENTATION (EXTENDED)

1️ PRIMARY KEY JUSTIFICATION
• dim_user → user_key
  → Uniquely identifies each user and avoids duplicate user_id values.

• dim_vehicle → vehicle_key
  → Uniquely identifies each vehicle record (model, battery, age).

• dim_station → station_key
  → Ensures each station (ID + location + charger type) is unique.

• dim_time → time_key
  → Uniquely identifies each start–end time combination for analysis.

• dim_environment → environment_key
  → Represents each unique temperature record.

• fact_charging_sessions → session_key
  → Defines the unique grain of the fact table — one record per charging session.

Reason:
Surrogate keys (SERIAL) provide stable, unique identifiers,
avoid natural key duplication, and simplify joins during ETL.

2️ FOREIGN KEY JUSTIFICATION
• user_key → dim_user(user_key)
  → Links each charging session to a specific user type.

• vehicle_key → dim_vehicle(vehicle_key)
  → Connects session to vehicle specifications for performance analysis.

• station_key → dim_station(station_key)
  → Associates data with charger type and city for location-based analysis.

• time_key → dim_time(time_key)
  → Enables time-based reporting (daily, monthly, yearly trends).

• environment_key → dim_environment(environment_key)
  → Supports analysis of charging patterns under different temperatures.

Reason:
Foreign keys maintain referential integrity between fact and dimensions,
supporting multi-dimensional OLAP queries.

3️ INDEXING RECOMMENDATIONS (For Performance)
• dim_time.start_time
  → Speeds up time-range queries and aggregations by month/year.

• dim_station.station_id
  → Improves performance of station-level lookups and joins.

• dim_user.user_id
  → Enhances ETL joins and user-based filtering during analysis.

Additional:
Index foreign keys (user_key, vehicle_key, station_key, time_key)
in the fact table to accelerate joins with dimensions.

4️ FACT TABLE GRAIN
Each record in fact_charging_sessions represents one
charging session per user, per vehicle, per station, per time.
This ensures accurate aggregation and prevents duplication
in analytical queries.
*/

