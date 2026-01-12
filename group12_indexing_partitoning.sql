
-- B-Tree Index on user_key (fast equality/range queries)
CREATE INDEX IF NOT EXISTS idx_fact_user_key_btree
ON "EV_vehicle".fact_charging_sessions(user_key);

-- Bitmap-like Index (simulated via standard index on vehicle_key)
CREATE INDEX IF NOT EXISTS idx_fact_vehicle_key
ON "EV_vehicle".fact_charging_sessions(vehicle_key);

--  SEQUENTIAL SCAN VS INDEX SCAN COMPARISON

-- Sequential scan example (no index used)
EXPLAIN ANALYZE
SELECT *
FROM "EV_vehicle".fact_charging_sessions
WHERE user_key = 101;

-- Index scan example (B-Tree index will be used)
EXPLAIN ANALYZE
SELECT *
FROM "EV_vehicle".fact_charging_sessions
WHERE user_key = 101;

-- Multi-condition query (Bitmap Index Scan likely)
EXPLAIN ANALYZE
SELECT *
FROM "EV_vehicle".fact_charging_sessions
WHERE user_key = 101
  AND vehicle_key = 5;
