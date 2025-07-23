# Traffic Analysis Pipeline

## Project Overview

This comprehensive data pipeline offers an in-depth approach to traffic congestion analysis. It incorporates daily data quality checks, performs intricate transformations to derive congestion metrics, and utilizes analytical models to pinpoint top congested locations. Furthermore, the pipeline examines speed and volume patterns as well as time-based trends, delivering valuable insights to traffic management teams.

## Data Sources

The primary dataset used for this analysis is the National Performance Management Research Data Set (NPMRDS). It provides detailed travel time data at 5-minute intervals. 

![link3_data_head](https://github.com/user-attachments/assets/1c8bcfc0-ee93-45cf-8836-b937906bf884)

## Implementation Details
### a. Airflow DAG for Pipeline Orchestration
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'traffic_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_dbt_models():
    import subprocess
    subprocess.run(["dbt", "run"], cwd="/opt/airflow/dbt/traffic")

def run_dbt_tests():
    import subprocess
    subprocess.run(["dbt", "test"], cwd="/opt/airflow/dbt/traffic")

with DAG(
    'traffic_congestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    extract_load = PostgresOperator(
        task_id='extract_load_raw_data',
        postgres_conn_id='traffic_db',
        sql='sql/extract_load_traffic_data.sql'
    )
    
    data_quality_checks = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=run_dbt_tests
    )
    
    transform_data = PythonOperator(
        task_id='transform_data_with_dbt',
        python_callable=run_dbt_models
    )
    
    generate_insights = PostgresOperator(
        task_id='generate_congestion_insights',
        postgres_conn_id='traffic_db',
        sql='sql/generate_insights.sql'
    )
    
    end = DummyOperator(task_id='end')
    
    start >> extract_load >> data_quality_checks >> transform_data >> generate_insights >> end
```  
##### DBT Structure 
```text
traffic_dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── schema.yml
│   │   ├── stg_traffic_volume.sql
│   │   ├── stg_traffic_speed.sql
│   │   └── stg_incidents.sql
│   ├── intermediate/
│   │   ├── int_congestion_metrics.sql
│   │   └── int_location_analysis.sql
│   └── marts/
│       ├── analytics/
│       │   ├── top_congested_locations.sql
│       │   ├── speed_analysis.sql
│       │   └── volume_analysis.sql
│       └── schema.yml
├── tests/
│   ├── quality_checks/
│   │   ├── volume_data_quality.sql
│   │   └── speed_data_quality.sql
└── macros/
    ├── calculate_congestion_index.sql
    └── time_buckets.sql
```
#### --1 dbt_project.yml
```yml
name: 'traffic_congestion'
version: '1.0.0'
config-version: 2

profile: 'traffic'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

vars:
  start_date: "2023-01-01"
  end_date: "2023-12-31"
  max_null_records: 100

models:
  traffic_congestion:
    staging:
      +schema: staging
      +materialized: view
    intermediate:
      +schema: intermediate
      +materialized: table
    marts:
      +schema: analytics
      +materialized: table

seeds:
  +schema: reference
  +materialized: table
```
#### --2. Staging Models
models/staging/stg_traffic_volume.sql
```sql
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH source_data AS (
    SELECT
        id as record_id,
        location_id,
        CAST(recorded_time AS TIMESTAMP) as recorded_time,
        vehicle_count,
        average_speed,
        lane_count,
        data_source
    FROM 
        {{ source('traffic', 'raw_volume_data') }}
    WHERE
        recorded_time BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
)

SELECT
    record_id,
    location_id,
    recorded_time,
    DATE_TRUNC('hour', recorded_time) as recorded_hour,
    -- Data quality checks
    CASE 
        WHEN vehicle_count < 0 THEN NULL 
        WHEN vehicle_count > 10000 THEN NULL  -- unrealistic high value
        ELSE vehicle_count 
    END as vehicle_count,
    CASE 
        WHEN average_speed < 0 THEN NULL 
        WHEN average_speed > 120 THEN NULL  -- assuming 120 mph is max speed limit
        ELSE average_speed 
    END as average_speed,
    lane_count,
    data_source,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    source_data
```
models/staging/stg_traffic_speed.sql
```sql
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH source_data AS (
    SELECT
        id as record_id,
        sensor_id as location_id,
        timestamp as recorded_time,
        speed as average_speed,
        vehicle_count,
        confidence_score
    FROM 
        {{ source('traffic', 'raw_speed_data') }}
    WHERE
        timestamp BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
)

SELECT
    record_id,
    location_id,
    recorded_time,
    DATE_TRUNC('hour', recorded_time) as recorded_hour,
    -- Data quality checks
    CASE 
        WHEN average_speed < 0 THEN NULL 
        WHEN average_speed > 120 THEN NULL  -- assuming 120 mph is max speed limit
        ELSE average_speed 
    END as average_speed,
    CASE 
        WHEN vehicle_count < 0 THEN NULL 
        ELSE vehicle_count 
    END as vehicle_count,
    confidence_score,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    source_data
```
models/staging/stg_incidents.sql
```sql
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
    incident_id,
    location_id,
    start_time as incident_start_time,
    end_time as incident_end_time,
    severity,
    type as incident_type,
    description,
    affected_lanes,
    -- Clean and standardize incident types
    CASE 
        WHEN LOWER(type) LIKE '%accident%' THEN 'Accident'
        WHEN LOWER(type) LIKE '%construction%' THEN 'Construction'
        WHEN LOWER(type) LIKE '%breakdown%' THEN 'Vehicle Breakdown'
        ELSE INITCAP(type)
    END as standardized_incident_type,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    {{ source('traffic', 'raw_incident_data') }}
WHERE
    start_time BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
```
models/staging/stg_locations.sql
```sql
{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
    location_id,
    name as location_name,
    latitude,
    longitude,
    road_name,
    road_type,
    direction,
    lanes,
    speed_limit,
    is_highway as highway_flag,
    is_intersection as intersection_flag,
    city,
    state,
    zip_code,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    {{ source('traffic', 'raw_location_data') }}
```
models/staging/schema.yml
```yml
version: 2

models:
  - name: stg_traffic_volume
    description: "Staging model for traffic volume data"
    columns:
      - name: location_id
        description: "Unique identifier for the location/sensor"
        tests:
          - not_null
          - relationships:
              to: ref('stg_locations')
              field: location_id
              severity: error
      - name: recorded_time
        description: "Timestamp when the measurement was taken"
        tests:
          - not_null
      - name: vehicle_count
        description: "Number of vehicles counted in the time period"
        tests:
          - not_null
          - accepted_values:
              min: 0
              max: 10000
              severity: warn
      - name: average_speed
        description: "Average speed of vehicles in mph"
        tests:
          - not_null
          - accepted_values:
              min: 0
              max: 120
              severity: warn

  - name: stg_traffic_speed
    description: "Staging model for traffic speed data"
    columns:
      - name: location_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_locations')
              field: location_id
      - name: average_speed
        tests:
          - not_null
          - accepted_values:
              min: 0
              max: 120

  - name: stg_incidents
    description: "Staging model for traffic incident data"
    columns:
      - name: location_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_locations')
              field: location_id
      - name: incident_start_time
        tests:
          - not_null

  - name: stg_locations
    description: "Staging model for location reference data"
    columns:
      - name: location_id
        tests:
          - not_null
          - unique
      - name: latitude
        tests:
          - not_null
          - accepted_values:
              min: -90
              max: 90
      - name: longitude
        tests:
          - not_null
          - accepted_values:
              min: -180
              max: 180
```
#### --3. Intermediate Models
models/intermediate/int_congestion_metrics.sql
```sql
{{
  config(
    materialized='table',
    schema='intermediate',
    partition_by={
      "field": "recorded_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["location_id"]
  )
}}

WITH speed_metrics AS (
    SELECT
        location_id,
        recorded_hour,
        AVG(average_speed) as avg_speed,
        PERCENTILE_CONT(average_speed, 0.5) OVER (PARTITION BY location_id, recorded_hour) as median_speed,
        MIN(average_speed) as min_speed,
        MAX(average_speed) as max_speed,
        COUNT(*) as speed_records_count
    FROM 
        {{ ref('stg_traffic_speed') }}
    WHERE
        average_speed IS NOT NULL
    GROUP BY
        location_id, recorded_hour
),

volume_metrics AS (
    SELECT
        location_id,
        recorded_hour,
        SUM(vehicle_count) as total_volume,
        AVG(vehicle_count) as avg_volume,
        MAX(vehicle_count) as max_volume,
        COUNT(*) as volume_records_count
    FROM 
        {{ ref('stg_traffic_volume') }}
    WHERE
        vehicle_count IS NOT NULL
    GROUP BY
        location_id, recorded_hour
),

incident_impact AS (
    SELECT
        location_id,
        DATE_TRUNC('hour', incident_start_time) as incident_hour,
        COUNT(*) as incident_count,
        MAX(severity) as max_severity
    FROM 
        {{ ref('stg_incidents') }}
    GROUP BY
        location_id, DATE_TRUNC('hour', incident_start_time)
)

SELECT
    COALESCE(s.location_id, v.location_id) as location_id,
    COALESCE(s.recorded_hour, v.recorded_hour) as recorded_hour,
    DATE(COALESCE(s.recorded_hour, v.recorded_hour)) as recorded_date,
    s.avg_speed,
    s.median_speed,
    s.min_speed,
    s.max_speed,
    s.speed_records_count,
    v.total_volume,
    v.avg_volume,
    v.max_volume,
    v.volume_records_count,
    COALESCE(i.incident_count, 0) as incident_count,
    i.max_severity,
    -- Calculate congestion index (custom metric)
    {{ calculate_congestion_index('v.total_volume', 's.avg_speed') }} as congestion_index,
    -- Calculate speed ratio (actual vs free flow)
    s.avg_speed / l.speed_limit as speed_ratio,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    speed_metrics s
FULL OUTER JOIN 
    volume_metrics v 
    ON s.location_id = v.location_id 
    AND s.recorded_hour = v.recorded_hour
LEFT JOIN
    incident_impact i
    ON COALESCE(s.location_id, v.location_id) = i.location_id
    AND COALESCE(s.recorded_hour, v.recorded_hour) = i.incident_hour
LEFT JOIN
    {{ ref('stg_locations') }} l
    ON COALESCE(s.location_id, v.location_id) = l.location_id
```
models/intermediate/int_location_analysis.sql
```sql
{{
  config(
    materialized='table',
    schema='intermediate'
  )
}}

WITH location_stats AS (
    SELECT
        l.location_id,
        l.location_name,
        l.road_type,
        l.lanes,
        l.speed_limit,
        l.highway_flag,
        l.intersection_flag,
        l.city,
        l.state,
        AVG(c.avg_speed) as avg_speed,
        AVG(c.total_volume) as avg_volume,
        AVG(c.congestion_index) as avg_congestion,
        COUNT(DISTINCT DATE(c.recorded_date)) as days_observed,
        SUM(c.incident_count) as total_incidents
    FROM 
        {{ ref('int_congestion_metrics') }} c
    JOIN 
        {{ ref('stg_locations') }} l ON c.location_id = l.location_id
    GROUP BY
        l.location_id, l.location_name, l.road_type, l.lanes, l.speed_limit,
        l.highway_flag, l.intersection_flag, l.city, l.state
)

SELECT
    *,
    -- Calculate congestion severity
    CASE
        WHEN avg_congestion > 0.8 THEN 'Severe'
        WHEN avg_congestion > 0.5 THEN 'High'
        WHEN avg_congestion > 0.3 THEN 'Moderate'
        ELSE 'Low'
    END as congestion_severity,
    -- Calculate incident rate
    total_incidents / NULLIF(days_observed, 0) as daily_incident_rate,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    location_stats
```
#### --4. Mart Models
models/marts/analytics/top_congested_locations.sql
```sql
{{
  config(
    materialized='table',
    schema='analytics',
    tags=['daily']
  )
}}

WITH daily_congestion AS (
    SELECT
        l.location_id,
        l.location_name,
        l.latitude,
        l.longitude,
        l.road_type,
        l.city,
        l.state,
        l.speed_limit,
        DATE(c.recorded_hour) as date,
        AVG(c.congestion_index) as avg_daily_congestion,
        AVG(c.avg_speed) as avg_daily_speed,
        SUM(c.total_volume) as daily_volume,
        MAX(c.incident_count) as daily_incidents
    FROM 
        {{ ref('int_congestion_metrics') }} c
    JOIN 
        {{ ref('stg_locations') }} l ON c.location_id = l.location_id
    WHERE
        DATE(c.recorded_hour) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  -- Yesterday's data
    GROUP BY
        l.location_id, l.location_name, l.latitude, l.longitude, l.road_type,
        l.city, l.state, l.speed_limit, DATE(c.recorded_hour)
)

SELECT
    location_id,
    location_name,
    road_type,
    city,
    state,
    latitude,
    longitude,
    speed_limit,
    date,
    ROUND(avg_daily_congestion, 3) as congestion_index,
    ROUND(avg_daily_speed, 1) as avg_speed_mph,
    daily_volume,
    daily_incidents,
    RANK() OVER (ORDER BY avg_daily_congestion DESC) as congestion_rank,
    -- Add metadata
    '{{ invocation_id }}' as batch_id,
    current_timestamp as loaded_at
FROM 
    daily_congestion
QUALIFY
    congestion_rank <= 10
ORDER BY
    congestion_rank
```
models/marts/analytics/speed_analysis.sql
```sql
{{
  config(
    materialized='table',
    schema='analytics',
    tags=['hourly']
  )
}}

WITH speed_by_hour AS (
    SELECT
        l.location_id,
        l.location_name,
        l.road_type,
        EXTRACT(HOUR FROM c.recorded_hour) as hour_of_day,
        AVG(c.avg_speed) as avg_speed,
        AVG(c.total_volume) as avg_volume,
        COUNT(*) as record_count
    FROM 
        {{ ref('int_congestion_metrics') }} c
    JOIN 
        {{ ref('stg_locations') }} l ON c.location_id = l.location_id
    WHERE
        DATE(c.recorded_hour) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
    GROUP BY
        l.location_id, l.location_name, l.road_type, EXTRACT(HOUR FROM c.recorded_hour)
),

speed_percentiles AS (
    SELECT
        location_id,
        location_name,
        road_type,
        hour_of_day,
        avg_speed,
        avg_volume,
        record_count,
        PERCENT_RANK() OVER (PARTITION BY hour_of_day ORDER BY avg_speed) as speed_percentile,
        -- Add metadata
        '{{ invocation_id }}' as batch_id,
        current_timestamp as loaded_at
    FROM 
        speed_by_hour
)

SELECT
    *,
    CASE
        WHEN speed_percentile < 0.1 THEN 'Very Slow'
        WHEN speed_percentile < 0.25 THEN 'Slow'
        WHEN speed_percentile < 0.75 THEN 'Normal'
        WHEN speed_percentile < 0.9 THEN 'Fast'
        ELSE 'Very Fast'
    END as speed_category
FROM 
    speed_percentiles
ORDER BY
    location_id, hour_of_day
```
models/marts/analytics/volume_analysis.sql
```sql
{{
  config(
    materialized='table',
    schema='analytics',
    tags=['daily']
  )
}}

WITH volume_trends AS (
    SELECT
        l.location_id,
        l.location_name,
        l.road_type,
        l.lanes,
        l.city,
        l.state,
        DATE(c.recorded_hour) as date,
        EXTRACT(DAYOFWEEK FROM c.recorded_hour) as day_of_week,
        CASE EXTRACT(DAYOFWEEK FROM c.recorded_hour)
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END as day_name,
        SUM(c.total_volume) as daily_volume,
        AVG(c.avg_speed) as avg_speed,
        MAX(c.incident_count) as incident_count
    FROM 
        {{ ref('int_congestion_metrics') }} c
    JOIN 
        {{ ref('stg_locations') }} l ON c.location_id = l.location_id
    WHERE
        DATE(c.recorded_hour) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    GROUP BY
        l.location_id, l.location_name, l.road_type, l.lanes, l.city, l.state,
        DATE(c.recorded_hour), EXTRACT(DAYOFWEEK FROM c.recorded_hour)
),

weekly_avg AS (
    SELECT
        location_id,
        location_name,
        road_type,
        lanes,
        city,
        state,
        day_of_week,
        day_name,
        AVG(daily_volume) as avg_weekly_volume,
        AVG(avg_speed) as avg_weekly_speed,
        SUM(incident_count) as total_incidents,
        COUNT(*) as days_count,
        -- Add metadata
        '{{ invocation_id }}' as batch_id,
        current_timestamp as loaded_at
    FROM 
        volume_trends
    GROUP BY
        location_id, location_name, road_type, lanes, city, state, day_of_week, day_name
)

SELECT
    *,
    avg_weekly_volume / lanes as volume_per_lane,
    RANK() OVER (PARTITION BY day_of_week ORDER BY avg_weekly_volume DESC) as volume_rank,
    CASE
        WHEN avg_weekly_volume > 100000 THEN 'Extremely High'
        WHEN avg_weekly_volume > 75000 THEN 'Very High'
        WHEN avg_weekly_volume > 50000 THEN 'High'
        WHEN avg_weekly_volume > 25000 THEN 'Medium'
        ELSE 'Low'
    END as volume_category
FROM 
    weekly_avg
ORDER BY
    location_id, day_of_week
```
models/marts/analytics/schema.yml
```yml
version: 2

models:
  - name: top_congested_locations
    description: "Top 10 most congested locations by day"
    columns:
      - name: congestion_index
        description: "Calculated congestion index (0-1 scale)"
        tests:
          - not_null
          - accepted_values:
              min: 0
              max: 1
      - name: congestion_rank
        description: "Rank of location by congestion (1 is most congested)"
        tests:
          - not_null
          - accepted_values:
              min: 1
              max: 10

  - name: speed_analysis
    description: "Speed analysis by hour of day"
    columns:
      - name: hour_of_day
        description: "Hour of day (0-23)"
        tests:
          - not_null
          - accepted_values:
              min: 0
              max: 23
      - name: speed_category
        description: "Categorical speed classification"

  - name: volume_analysis
    description: "Volume trends by day of week"
    columns:
      - name: day_of_week
        description: "Day of week (1=Sunday)"
        tests:
          - not_null
          - accepted_values:
              min: 1
              max: 7
      - name: volume_per_lane
        description: "Average daily volume per lane"
```
#### --5. Macros
macros/calculate_congestion_index.sql
```sql
{% macro calculate_congestion_index(volume_column, speed_column) %}
    -- Custom congestion index calculation
    -- Parameters:
    --   volume_column: Column with traffic volume data
    --   speed_column: Column with speed data
    
    -- Formula: (1 - normalized_speed) * volume_weight
    -- Where normalized_speed is speed as fraction of speed limit
    -- And volume_weight is volume as fraction of capacity (assuming 2000 vehicles per lane per hour as capacity)
    
    CASE
        WHEN {{ speed_column }} IS NULL OR {{ volume_column }} IS NULL THEN NULL
        WHEN {{ speed_column }} <= 0 THEN 1.0  -- Max congestion when speed is 0
        ELSE 
            LEAST(
                (1.0 - ({{ speed_column }} / NULLIF(l.speed_limit, 0))) * 
                ({{ volume_column }} / NULLIF((2000 * l.lanes), 1)),
                1.0
            )
    END
{% endmacro %}
```
macros/time_buckets.sql
```sql
{% macro time_buckets(time_column) %}
    -- Create time buckets for analysis
    CASE
        WHEN EXTRACT(HOUR FROM {{ time_column }}) BETWEEN 6 AND 9 THEN 'Morning Rush (6-9am)'
        WHEN EXTRACT(HOUR FROM {{ time_column }}) BETWEEN 15 AND 18 THEN 'Evening Rush (3-6pm)'
        WHEN EXTRACT(HOUR FROM {{ time_column }}) BETWEEN 11 AND 13 THEN 'Midday (11am-1pm)'
        WHEN EXTRACT(HOUR FROM {{ time_column }}) BETWEEN 19 AND 22 THEN 'Evening (7-10pm)'
        WHEN EXTRACT(HOUR FROM {{ time_column }}) BETWEEN 22 AND 24 OR 
             EXTRACT(HOUR FROM {{ time_column }}) BETWEEN 0 AND 5 THEN 'Overnight (10pm-5am)'
        ELSE 'Other Daytime'
    END
{% endmacro %}
```
#### -- 6. Tests
tests/quality_checks/volume_data_quality.sql
```sql
-- Test for missing volume data
SELECT
    COUNT(*) as missing_volume_records
FROM 
    {{ ref('stg_traffic_volume') }}
WHERE
    vehicle_count IS NULL
    OR average_speed IS NULL
    OR location_id IS NULL

HAVING
    COUNT(*) > {{ var('max_null_records', 100) }}
```
tests/quality_checks/speed_data_quality.sql
```sql
-- Test for speed outliers
WITH speed_stats AS (
    SELECT
        STDDEV(average_speed) as speed_stddev,
        AVG(average_speed) as speed_avg
    FROM 
        {{ ref('stg_traffic_speed') }}
    WHERE
        average_speed IS NOT NULL
)

SELECT
    COUNT(*) as outlier_speed_records
FROM 
    {{ ref('stg_traffic_speed') }} s,
    speed_stats
WHERE
    s.average_speed IS NOT NULL
    AND (s.average_speed > speed_avg + 3 * speed_stddev
         OR s.average_speed < speed_avg - 3 * speed_stddev)

HAVING
    COUNT(*) > {{ var('max_outlier_records', 50) }}
```
tests/quality_checks/incident_consistency.sql
```sql
-- Test for incidents with end time before start time
SELECT
    incident_id,
    incident_start_time,
    incident_end_time
FROM 
    {{ ref('stg_incidents') }}
WHERE
    incident_end_time < incident_start_time

HAVING
    COUNT(*) > 0
```
