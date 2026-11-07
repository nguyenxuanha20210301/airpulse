-- views for the dashboards, trino joins the hive tables on hdfs with the cassandra tables in
-- one query. catalogs are named hive and cassandra in deploy/helm/trino. ref_locations holds the
-- name, province and region of every point, copied from the config by register_tables.

CREATE SCHEMA IF NOT EXISTS hive.airpulse_views;

-- the realtime ranking, newest clean value per point with the computed index.
CREATE OR REPLACE VIEW hive.airpulse_views.current_air AS
SELECT c.location_id, l.name, l.province, l.region, c.event_ts_utc, c.pm2_5_ugm3, c.pm10_ugm3,
       c.no2_ugm3, c.o3_ugm3, c.aqi_computed, c.aqi_dominant, c.aqi_category, c.aqi_us
FROM cassandra.airpulse.rt_air_latest c
LEFT JOIN hive.airpulse.ref_locations l ON l.location_id = c.location_id;

-- the live value next to the long term mean of the same point and calendar month, the speed
-- layer and the batch layer merged in one query.
CREATE OR REPLACE VIEW hive.airpulse_views.current_vs_history AS
SELECT c.location_id, l.name, l.province, l.region, c.event_ts_utc, c.pm2_5_ugm3,
       c.aqi_computed, c.aqi_category, round(h.mean_pm2_5, 1) AS monthly_mean_pm2_5,
       round(c.pm2_5_ugm3 - h.mean_pm2_5, 1) AS pm2_5_above_mean, h.n_years
FROM cassandra.airpulse.rt_air_latest c
LEFT JOIN (
  SELECT location_id, month, avg(avg_pm2_5) AS mean_pm2_5, count(*) AS n_years
  FROM hive.airpulse.curated_air_monthly
  GROUP BY location_id, month
) h ON h.location_id = c.location_id AND h.month = month(c.event_ts_utc)
LEFT JOIN hive.airpulse.ref_locations l ON l.location_id = c.location_id;

-- the newest forecast per point, target and horizon.
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

-- alerts of the last two days across every point.
CREATE OR REPLACE VIEW hive.airpulse_views.recent_alerts AS
SELECT alert_day, event_ts_utc, location_id, kind, metric, value, threshold, severity
FROM cassandra.airpulse.rt_alerts
WHERE alert_day >= cast(date_add('day', -1, current_date) AS varchar);

-- one row per point and day with air quality and weather side by side. the full join keeps
-- weather days older than the air quality archive, and the exceedance flags become numbers so
-- shares and day counts are plain averages and sums.
CREATE OR REPLACE VIEW hive.airpulse_views.daily_by_point AS
SELECT CAST(coalesce(a.dt, w.dt) AS date) AS day,
       coalesce(a.location_id, w.location_id) AS location_id,
       l.name, l.province, l.region,
       a.avg_pm2_5, a.max_pm2_5, a.p95_pm2_5, a.avg_pm10, a.avg_no2, a.avg_o3,
       a.avg_aqi, a.max_aqi,
       CAST(a.above_who AS integer) AS above_who,
       CAST(a.above_qcvn AS integer) AS above_qcvn,
       w.avg_temperature, w.min_temperature, w.max_temperature, w.sum_precipitation,
       w.avg_humidity, w.avg_wind_speed
FROM hive.airpulse.curated_air_daily a
FULL JOIN hive.airpulse.curated_weather_daily w
  ON w.location_id = a.location_id AND w.dt = a.dt
LEFT JOIN hive.airpulse.ref_locations l
  ON l.location_id = coalesce(a.location_id, w.location_id);

-- the month by local hour profile of PM2.5 per point. the labels are zero padded so they sort
-- in calendar order, and n_hours weights the means when points or months are combined.
CREATE OR REPLACE VIEW hive.airpulse_views.heatmap_by_point AS
SELECT h.month, format('%02d', h.month) AS month_label,
       h.hour_local, format('%02d', h.hour_local) AS hour_label, h.hour_utc,
       h.location_id, l.name, l.province, l.region, h.avg_pm2_5, h.n_hours
FROM hive.airpulse.curated_heatmap_month_hour h
LEFT JOIN hive.airpulse.ref_locations l ON l.location_id = h.location_id;

-- correlations of hourly PM2.5 with each weather variable. rows of the region scope carry the
-- region in the key, rows of the point scopes get it from the reference table, so the region
-- and province filters of the dashboard apply to both.
CREATE OR REPLACE VIEW hive.airpulse_views.pm2_5_correlations AS
SELECT c.scope, c.variable,
       replace(regexp_replace(c.variable, '_(kmh|pct|c|mm|hpa)$', ''), '_', ' ') AS weather_variable,
       c.correlation, c.n,
       CASE WHEN c.scope = 'region' THEN c.key ELSE l.region END AS region,
       l.province, l.name,
       CASE WHEN c.scope IN ('location', 'location_month') THEN split_part(c.key, '|', 1) END AS location_id,
       CASE WHEN c.scope = 'location_month' THEN CAST(split_part(c.key, '|', 2) AS integer) END AS month
FROM hive.airpulse.curated_correlations c
LEFT JOIN hive.airpulse.ref_locations l
  ON c.scope IN ('location', 'location_month') AND l.location_id = split_part(c.key, '|', 1);

-- holdout errors of the forecast model and the baselines, the horizon label sorts in order.
CREATE OR REPLACE VIEW hive.airpulse_views.forecast_errors AS
SELECT target, horizon_h, format('+%02dh', horizon_h) AS horizon, model,
       replace(model, '_', ' ') AS model_name, mae, rmse, n, model_version
FROM hive.airpulse.curated_forecast_evaluation;
