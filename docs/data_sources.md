# Data sources

| Source | What | Access | Role |
|---|---|---|---|
| Open-Meteo forecast API | hourly weather, temperature, humidity, dew point, apparent temperature, precipitation, rain, weather code, pressure, cloud cover, wind | free, no key, about ten thousand calls per day | main live weather source |
| Open-Meteo air quality API (CAMS) | hourly PM2.5, PM10, CO, NO2, SO2, O3, dust, UV index, US and european AQI | free, no key | main live air quality source, also provides history |
| Open-Meteo archive API (ERA5) | hourly weather since 1940 | free, no key | backfill of many years |
| OpenAQ v3 | station measurements, mostly PM2.5 in Vietnam | free with an api key | real measurements next to the model values |
| OpenWeatherMap | current weather and air pollution | free plan with an api key, sixty calls per minute | backup and cross check |

## Sampling points

`configs/locations/vn_provinces.csv` lists 63 points, the centers of the former provinces,
with the region and the province they belong to after the 2025 merger. Both groupings are
available in the curated tables.

## Request budget

A live cycle every 30 minutes with ten points per request costs 14 calls per cycle, under 700
calls per day for the two Open-Meteo APIs together. The backfill fetches one month and ten
points per call, so ten years of weather and air for 63 points is about 1700 calls.

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
