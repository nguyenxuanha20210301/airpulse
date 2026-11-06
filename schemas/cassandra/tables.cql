-- tables of the realtime view, written by the speed layer through the spark connector.
-- column names match the clean event columns one to one, the connector maps them by name.

CREATE TABLE IF NOT EXISTS airpulse.rt_air_latest (
  location_id text PRIMARY KEY,
  event_id text,
  schema_version int,
  source text,
  station_id text,
  lat double,
  lon double,
  event_ts_utc timestamp,
  ingested_ts_utc timestamp,
  run_id text,
  clean_run_id text,
  pm2_5_ugm3 double,
  pm10_ugm3 double,
  co_ugm3 double,
  no2_ugm3 double,
  so2_ugm3 double,
  o3_ugm3 double,
  dust_ugm3 double,
  uv_index double,
  aqi_us int,
  aqi_eu int,
  aqi_computed int,
  aqi_dominant text,
  aqi_category text
);

CREATE TABLE IF NOT EXISTS airpulse.rt_weather_latest (
  location_id text PRIMARY KEY,
  event_id text,
  schema_version int,
  source text,
  station_id text,
  lat double,
  lon double,
  event_ts_utc timestamp,
  ingested_ts_utc timestamp,
  run_id text,
  clean_run_id text,
  temperature_c double,
  humidity_pct double,
  dew_point_c double,
  apparent_temperature_c double,
  precipitation_mm double,
  rain_mm double,
  weather_code int,
  pressure_msl_hpa double,
  surface_pressure_hpa double,
  cloud_cover_pct double,
  wind_speed_kmh double,
  wind_direction_deg double,
  wind_gusts_kmh double
);

-- one partition per location and window size, newest window first, a week of history.
CREATE TABLE IF NOT EXISTS airpulse.rt_air_windows (
  location_id text,
  window_size text,
  window_start timestamp,
  window_end timestamp,
  avg_pm2_5 double,
  max_pm2_5 double,
  std_pm2_5 double,
  avg_pm10 double,
  avg_no2 double,
  avg_o3 double,
  avg_aqi double,
  max_aqi int,
  n_samples bigint,
  PRIMARY KEY ((location_id, window_size), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS airpulse.rt_weather_windows (
  location_id text,
  window_size text,
  window_start timestamp,
  window_end timestamp,
  avg_temperature double,
  min_temperature double,
  max_temperature double,
  avg_humidity double,
  avg_wind_speed double,
  sum_precipitation double,
  n_samples bigint,
  PRIMARY KEY ((location_id, window_size), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;

-- partitioned by day so a dashboard reads the recent alerts of every location in one query.
CREATE TABLE IF NOT EXISTS airpulse.rt_alerts (
  alert_day text,
  event_ts_utc timestamp,
  location_id text,
  kind text,
  alert_id text,
  schema_version int,
  detected_ts_utc timestamp,
  metric text,
  value double,
  threshold double,
  severity text,
  run_id text,
  PRIMARY KEY (alert_day, event_ts_utc, location_id, kind)
) WITH CLUSTERING ORDER BY (event_ts_utc DESC, location_id ASC, kind ASC)
  AND default_time_to_live = 2592000;

-- one partition per location, target and horizon, the first row is the newest forecast.
CREATE TABLE IF NOT EXISTS airpulse.rt_forecast (
  location_id text,
  target text,
  horizon_h int,
  event_ts_utc timestamp,
  forecast_ts_utc timestamp,
  predicted double,
  model_version text,
  run_id text,
  PRIMARY KEY ((location_id, target, horizon_h), event_ts_utc)
) WITH CLUSTERING ORDER BY (event_ts_utc DESC)
  AND default_time_to_live = 604800;
