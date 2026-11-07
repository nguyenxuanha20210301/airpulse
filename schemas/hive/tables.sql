-- external tables over the parquet datasets on hdfs, registered in the hive metastore by
-- python -m airpulse.serving.register_tables, which replaces ${root} with the hdfs root of the
-- profile and recovers the dt partitions. columns are inferred from the parquet files, so this
-- file only lists which dataset becomes which table.

CREATE DATABASE IF NOT EXISTS airpulse;

CREATE TABLE IF NOT EXISTS airpulse.raw_weather_hourly USING parquet LOCATION '${root}/raw/weather_hourly';
CREATE TABLE IF NOT EXISTS airpulse.raw_air_hourly USING parquet LOCATION '${root}/raw/air_hourly';
CREATE TABLE IF NOT EXISTS airpulse.clean_weather_hourly USING parquet LOCATION '${root}/clean/weather_hourly';
CREATE TABLE IF NOT EXISTS airpulse.clean_air_hourly USING parquet LOCATION '${root}/clean/air_hourly';
CREATE TABLE IF NOT EXISTS airpulse.curated_features USING parquet LOCATION '${root}/curated/features';
CREATE TABLE IF NOT EXISTS airpulse.curated_air_daily USING parquet LOCATION '${root}/curated/air_daily';
CREATE TABLE IF NOT EXISTS airpulse.curated_weather_daily USING parquet LOCATION '${root}/curated/weather_daily';
CREATE TABLE IF NOT EXISTS airpulse.curated_air_monthly USING parquet LOCATION '${root}/curated/air_monthly';
CREATE TABLE IF NOT EXISTS airpulse.curated_weather_monthly USING parquet LOCATION '${root}/curated/weather_monthly';
CREATE TABLE IF NOT EXISTS airpulse.curated_heatmap_month_hour USING parquet LOCATION '${root}/curated/heatmap_month_hour';
CREATE TABLE IF NOT EXISTS airpulse.curated_seasonal USING parquet LOCATION '${root}/curated/seasonal';
CREATE TABLE IF NOT EXISTS airpulse.curated_yearly_trend USING parquet LOCATION '${root}/curated/yearly_trend';
CREATE TABLE IF NOT EXISTS airpulse.curated_correlations USING parquet LOCATION '${root}/curated/correlations';
CREATE TABLE IF NOT EXISTS airpulse.curated_forecast_evaluation USING parquet LOCATION '${root}/curated/forecast_evaluation';
CREATE TABLE IF NOT EXISTS airpulse.curated_mapreduce_monthly_pm2_5 USING parquet LOCATION '${root}/curated/mapreduce_monthly_pm2_5';
CREATE TABLE IF NOT EXISTS airpulse.ref_locations USING parquet LOCATION '${root}/reference/locations';
