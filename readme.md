# NYC Taxi Analytics – ETL with Airflow, PySpark & Snowflake

This project implements an end-to-end analytics pipeline for NYC Yellow Taxi data using:

- **Apache Airflow** – orchestration  
- **PySpark** – batch ETL for historical trips  
- **Snowflake** – data warehouse  
- **OpenWeather API** – near real-time weather feed  

The data from these pipelines is later modeled in dbt and visualized in BI tools.

---

## Architecture Overview

### Data Sources

- **Historical:** NYC TLC Yellow Taxi trip data (monthly Parquet files).  
- **Real-time:** Current weather in New York City from the OpenWeather API.

### Airflow DAGs

1. **Historical ETL – `nyc_taxi_pyspark_etl`**
   - Finds the most recent available TLC Parquet file for the current year by probing month URLs.
   - Downloads and cleans it with PySpark (filters invalid rows, selects key columns).
   - Writes a cleaned CSV to a `/tmp` directory inside the Airflow container.
   - Loads data into Snowflake using:
     - an internal stage (PUT from local file to Snowflake),
     - a staging table for bulk loads,
     - and a MERGE step into the main fact table to avoid duplicates.
   - All Snowflake work is done via `SnowflakeHook` with explicit transactions (commit / rollback), try-catch with raise error and logging.

2. **Real-Time Weather ETL – `nyc_weather_realtime_etl`**
   - Calls OpenWeather’s **Current Weather** API on a schedule (for example, hourly).
   - Reads the city and API key from Airflow Variables.
   - Parses temperature, humidity, weather description, and keeps the raw JSON payload.
   - Inserts one row per run into a Snowflake table that stores weather observations.
   - Also uses `SnowflakeHook` with proper error handling and transactions.

### Snowflake Storage

All raw data lands in a Snowflake **raw schema** (for example, `RAW`), including:

- A fact table for cleaned taxi trips (historical).  
- A staging table used during COPY/MERGE of trip data.  
- A fact table for weather observations (one row per DAG run).  

These raw tables become the foundation for downstream dbt models and BI dashboards.

---

## Configuration

### Snowflake Connection in Airflow

In the Airflow **Connections** UI, create a Snowflake connection (for example named `snowflake_catfish`) with:

- account  
- user  
- password or key  
- warehouse  
- database  
- role  

The DAGs reference this connection ID via `SnowflakeHook`. No credentials are stored in this repository.

### Airflow Variables

Create the following Airflow Variables (in **Admin → Variables**):

- `target_schema_raw`  
  - Example value: `RAW`  
  - Used by both DAGs as the target schema for raw tables.

- `openweather_api_key`  
  - Your OpenWeather API key (see below).  
  - Used by the weather DAG to authenticate with the OpenWeather API.

- `weather_city` *(optional)*  
  - Example value: `New York`  
  - If not set, the weather DAG defaults to “New York”.

---

## OpenWeather API Key Setup

1. Sign up and log in to OpenWeather.  
2. Confirm your email address (required before keys work).  
3. Go to the **API keys** page in your OpenWeather account dashboard.  
4. Create or copy an API key.  
5. Test your key in a browser by calling the Current Weather endpoint for New York with your key and units set to imperial (Fahrenheit). You should receive a JSON response with weather data, not a 401 error.
6. In Airflow → **Admin → Variables**:
   - Create a variable with key `openweather_api_key`.  
   - Set the value to your API key token (only the token, no quotes or prefixes).



---

## Verifying the Pipelines in Snowflake (Conceptual Checks)

After running the DAGs, you can validate the results in Snowflake by:

- **Taxi trips table (historical ETL):**
  - Ensure the table in the raw schema exists.
  - Check that it has a non-zero row count.
  - Inspect sample rows to confirm pickup/dropoff timestamps, zones, distances, and amounts look reasonable.
  - Confirm the date range matches the month you expect (based on the file the DAG selected).

- **Weather observations table (real-time ETL):**
  - Ensure the table exists in the raw schema.
  - Verify that each run of `nyc_weather_realtime_etl` adds a new row.
  - Check that observed timestamps and load timestamps are recent.
  - Confirm temperature, humidity, and descriptions look realistic for the chosen city.

You can perform these checks with simple “select a few rows” and “count rows” queries in the Snowflake worksheet, without needing any special tooling.

---


## How This Supports the Project Requirements

This setup directly addresses the core project requirements:

- Uses **two data sources**:
  - A historical archive (NYC taxi trip data) loaded in batch via PySpark and Airflow.
  - A near real-time feed (weather conditions) pulled from the OpenWeather API.

- Demonstrates:
  - **Airflow** for orchestration and scheduling.  
  - **PySpark** for transformation of large historical files.  
  - **Snowflake** as the central cloud data warehouse.

- Produces raw fact tables that can be:
  - Joined (trips + weather) by time and zone in dbt models.
  - Used to analyze demand and delays by weather.
  - Used to power simple forecasting and live indicators in BI dashboards.
 
## Verifying the Pipelines in Snowflake

After your Airflow DAGs run, you can confirm that everything worked by checking the tables in Snowflake.

### 1. Verify Taxi Trips (Historical ETL)

In a Snowflake worksheet, run simple checks against your raw schema (for example `RAW`):

- Check that the trips table exists and has rows:

```sql

SELECT COUNT(*) AS trip_count
FROM RAW.NYC_TAXI_TRIPS;

Inspect a few sample rows:

SELECT *
FROM RAW.NYC_TAXI_TRIPS
LIMIT 20;


Confirm the date range of the loaded data:

SELECT
  MIN(PICKUP_DATETIME) AS first_pickup,
  MAX(PICKUP_DATETIME) AS last_pickup
FROM RAW.NYC_TAXI_TRIPS;


You should see a reasonable number of rows, and the pickup dates should match the most recent month that your ETL DAG downloaded. 
---

 2. Verify Weather Observations (Real-Time ETL)

For the real-time weather DAG (nyc_weather_realtime_etl), check that the weather table is being populated:

Confirm there are rows, and see the latest entries:

SELECT
  OBSERVED_AT,
  CITY,
  TEMP_F,
  WEATHER_DESC,
  HUMIDITY_PCT,
  LOAD_TS
FROM RAW.RAW_WEATHER
ORDER BY LOAD_TS DESC
LIMIT 10;


You should see one new row each time the DAG runs, with LOAD_TS and OBSERVED_AT close to the Airflow execution time, and realistic values for temperature, humidity, and description.

