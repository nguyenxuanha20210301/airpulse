-- views for the dashboards, trino joins the hive tables on hdfs with the cassandra tables in
-- one query. catalogs are named hive and cassandra in deploy/helm/trino.

CREATE SCHEMA IF NOT EXISTS hive.airpulse_views;

-- the realtime ranking, newest clean value per province with the computed index.
CREATE OR REPLACE VIEW hive.airpulse_views.current_air AS
SELECT location_id, event_ts_utc, pm2_5_ugm3, pm10_ugm3, no2_ugm3, o3_ugm3,
       aqi_computed, aqi_dominant, aqi_category, aqi_us
FROM cassandra.airpulse.rt_air_latest;

-- the live value next to the long term monthly mean of the same province and month.
CREATE OR REPLACE VIEW hive.airpulse_views.current_vs_history AS
SELECT c.location_id, c.event_ts_utc, c.pm2_5_ugm3, c.aqi_computed,
       h.avg_pm2_5 AS monthly_mean_pm2_5, h.days_above_who
FROM cassandra.airpulse.rt_air_latest c
LEFT JOIN hive.airpulse.curated_air_monthly h
  ON h.location_id = c.location_id
 AND h.month = month(c.event_ts_utc)
 AND h.year = year(c.event_ts_utc) - 1;

-- the newest forecast per province, target and horizon.
CREATE OR REPLACE VIEW hive.airpulse_views.latest_forecast AS
SELECT location_id, target, horizon_h, event_ts_utc, forecast_ts_utc, predicted, model_version
FROM (
  SELECT *, row_number() OVER (PARTITION BY location_id, target, horizon_h ORDER BY event_ts_utc DESC) AS rn
  FROM cassandra.airpulse.rt_forecast
)
WHERE rn = 1;

-- forecasts joined with what actually happened, for the forecast versus actual chart.
CREATE OR REPLACE VIEW hive.airpulse_views.forecast_vs_actual AS
SELECT f.location_id, f.target, f.horizon_h, f.forecast_ts_utc, f.predicted,
       CASE f.target WHEN 'pm2_5' THEN a.pm2_5_ugm3 ELSE w.temperature_c END AS actual
FROM cassandra.airpulse.rt_forecast f
LEFT JOIN hive.airpulse.clean_air_hourly a
  ON a.location_id = f.location_id AND a.event_ts_utc = f.forecast_ts_utc
LEFT JOIN hive.airpulse.clean_weather_hourly w
  ON w.location_id = f.location_id AND w.event_ts_utc = f.forecast_ts_utc;

-- alerts of the last two days across every province.
CREATE OR REPLACE VIEW hive.airpulse_views.recent_alerts AS
SELECT alert_day, event_ts_utc, location_id, kind, metric, value, threshold, severity
FROM cassandra.airpulse.rt_alerts
WHERE alert_day >= cast(date_add('day', -1, current_date) AS varchar);
