# superset dashboards

superset exports are produced from a running instance, so this folder is filled once the
cluster is up. the charts are built on the trino connection `trino://admin@trino:8080/hive`
and the views in `schemas/trino/views.sql`.

suggested charts for the analytics dashboard

- monthly mean PM2.5 per province, line chart on `curated_air_monthly`
- seasonality heatmap, month by hour on `curated_heatmap_month_hour`
- regions compared, box plot on `curated_air_daily` joined with the province list
- weather and pollution correlation, bar chart on `curated_correlations` with scope location
- days above WHO and QCVN limits per year, stacked bars on `curated_air_monthly`
- forecast errors, table on `curated_forecast_evaluation`

export with the superset cli once the dashboard exists, for example
`superset export-dashboards -f dashboards/superset/airpulse.zip`.
