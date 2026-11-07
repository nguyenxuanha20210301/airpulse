# Data sources

| Source | What | Access | Role |
|---|---|---|---|
| Open-Meteo forecast API | hourly weather, temperature, humidity, dew point, apparent temperature, precipitation, rain, weather code, pressure, cloud cover, wind | free, no key, about ten thousand calls per day | main live weather source and the last three months of history |
| Open-Meteo air quality API (CAMS) | hourly PM2.5, PM10, CO, NO2, SO2, O3, dust, UV index, US and european AQI | free, no key | main live air quality source, also provides history |
| Open-Meteo archive API (ERA5) | hourly weather since 1940 | free, no key, the most rate limited endpoint | backfill older than three months |
| OpenAQ v3 | station measurements, mostly PM2.5 in Vietnam | free with an api key | real measurements next to the model values |
| OpenWeatherMap | current weather and air pollution | free plan with an api key, sixty calls per minute | backup and cross check |

## Sampling points

`configs/locations/vn_provinces.csv` lists 63 points, the centers of the former provinces,
with the region and the province they belong to after the 2025 merger. Both groupings are
available in the curated tables.

## Request budget

The free Open-Meteo plan allows 10,000 calls a day, 5,000 an hour and 600 a minute, and a
request with more than ten variables or more than two weeks of data counts as several calls,
for example two weeks with fifteen variables count as 1.5 calls. A live cycle every 30 minutes
with ten points per request sends 14 requests, under 700 requests a day for the two APIs
together. The backfill sends one request per month and group of ten points, 168 requests
per year of weather and air, each weighted for its month of data and its thirteen weather or
ten air variables, so a few years fit in a day of quota but decades do not.

Recent weather history, the last `forecast_past_days` days (56), is fetched from the forecast
endpoint, which keeps past hours from the same models as the live feed. It accepts start dates
three months back but answers with empty hours beyond about eight weeks, checked on 2026-11-07
when values started on 2026-09-10. Older history goes to the archive endpoint (ERA5), the
busiest one of the free plan. While the demo cluster was built the archive answered
`429 Too many concurrent requests` for every request, the forecast and air quality endpoints
kept working.

Air quality history from the CAMS global model starts in early August 2022, the API answers
earlier dates with empty hours, mid August 2022 already has values. Long term pollution trends
therefore cover about four years, while weather history goes back decades.

## Canonical schema

Every source maps its own names and units to the columns of `WeatherEvent` and `AirEvent` in
`src/airpulse/common/schemas.py`. Units are part of the column names, `temperature_c`,
`wind_speed_kmh`, `pm2_5_ugm3`. OpenWeatherMap wind in m/s is converted, OpenAQ gases in ppm
or ppb are converted to micrograms per cubic meter, and provider specific codes that do not
share a scale are left empty.

## Known challenges

- Heterogeneous schemas and units across sources, handled by the mapping in each source.
- Missing values and sensor errors, handled by range checks that null single values rather
  than dropping rows, see `src/airpulse/common/cleaning.py`.
- Duplicates and late data, handled by the `event_id` and a two hour watermark in streaming,
  and by exact deduplication in the nightly batch.
- Rate limits, handled by batching points per request, retries with backoff on 429 and a
  throttle on the OpenWeatherMap client.
