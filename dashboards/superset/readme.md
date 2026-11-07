# superset dashboards

`assets/` is the export of the analytics dashboard, unpacked so every chart is a readable YAML
file. it holds the database connection `airpulse trino` (`trino://admin@trino:8080/hive`), five
datasets on the views of `schemas/trino/views.sql`, eighteen charts and the dashboard
`AirPulse analytics` with the slug `airpulse-analytics`.

## load and save

```bash
sh deploy/scripts/import_dashboards.sh dev   # repo to superset, objects matched by uuid and overwritten
sh deploy/scripts/export_dashboards.sh dev   # superset to repo, after editing in the ui
```

`make demo` runs the import after the views are applied. the charts query the views on every
load, so they follow whatever history is on HDFS and in Cassandra.

## datasets

| dataset | built on | used for |
|---|---|---|
| `daily_by_point` | `curated_air_daily` full join `curated_weather_daily`, with `ref_locations` | levels, exceedance days, monthly weather |
| `heatmap_by_point` | `curated_heatmap_month_hour` with `ref_locations` | month by local hour profile |
| `pm2_5_correlations` | `curated_correlations` with `ref_locations` | weather and pollution correlations |
| `forecast_errors` | `curated_forecast_evaluation` | model against baselines |
| `current_vs_history` | `rt_air_latest` in Cassandra joined with `curated_air_monthly` | latest value against history |

saved metrics carry the aggregation, for example `mean_pm2_5` is `AVG(avg_pm2_5)` and
`weighted_pm2_5` weights the heatmap cells by their number of hours.

## tabs

| tab | charts |
|---|---|
| pollution | points monitored, mean PM2.5, share of point days above the WHO guideline and the QCVN limit, daily mean by region with a seven day rolling mean, distribution of daily values by region, twenty points with the highest mean, days above both limits for the twenty worst points |
| seasons and weather | PM2.5 by month and local hour, correlation of hourly PM2.5 with wind, humidity, temperature, rain, pressure and cloud cover by region, monthly PM2.5, rainfall and temperature by region |
| forecast | MAE of the gradient boosted trees against the persistence, moving average and same hour yesterday baselines at 1, 6 and 24 hours, for PM2.5 and temperature, and the full error table |
| now against history | latest AQI of the ten worst points, latest PM2.5 of every point next to the mean of the same calendar month in the batch layer |

the filters region, province and period apply to every chart whose dataset has the column,
the others are left untouched. regions and models keep the same color in every chart.

## notes

- the WHO guideline of 15 and the QCVN limit of 50 for the daily mean come from `aqi` in
  `configs/base.yaml`, the curated tables already carry the flags.
- the CAMS model behind Open-Meteo has a grid of about 40 km, nearby points such as My Tho, Tan
  An and Ben Tre fall in the same cell and show identical values.
- superset names the exported files after chart titles, the export script lowercases them and
  drops the numeric id of the instance, so renaming a chart renames its file.
- the directory import creates objects without an owner, the header then shows not available,
  admins can still edit them and the owner can be set in the properties of the dashboard.
