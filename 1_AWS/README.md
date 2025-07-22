# Traffic Analysis Pipeline

## Project Overview

The purpose of this project was to create a fully automated ETL pipeline using Airflow, AWS S3, and RDS, running monthly to generate the Top 10 TTI (Travel Time Index) and PTI (Planning Time Index) congested roads:

## Data Sources

The primary dataset used for this analysis is the National Performance Management Research Data Set (NPMRDS). It provides detailed travel time data at 5-minute intervals. 

![link3_data_head](https://github.com/user-attachments/assets/1c8bcfc0-ee93-45cf-8836-b937906bf884)

## Architecture Details
## Data Sources
-NPMRDS traffic data in CSV (on S3)

## ETL Pipeline (Monthly)
-Extract: Pull monthly data from S3
-Transform: Calculate TTI and PTI for each TMC
-Load: Save results to AWS RDS (PostgreSQL/MySQL) or Snowflake
-Report: Identify top 10 congested (TTI) & worst roads (PTI)

### -- Airflow DAG Pipeline
The pipeline uses S3Hook, Pandas, and SQLAlchemy to process the data:

### --1. Install Dependencies
```python
pip install apache-airflow[aws] pandas numpy sqlalchemy psycopg2-binary
```
### --2. Airflow DAG Script
```python
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np
import io

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'monthly_congestion_analysis',
    default_args=default_args,
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['traffic', 'congestion', 'npmrds']
) as dag:

    @task
    def extract_data(year: int, month: int):
        s3 = S3Hook(aws_conn_id='aws_default')
        bucket = 'your-s3-bucket'
        prefix = f'npmrds/{year}/{month:02d}/'

        tt_file = f'{prefix}HITTAV{year}_{month:02d}.csv'
        shp_file = f'{prefix}TMC_Identification{year}_{month:02d}.csv'

        tt_obj = s3.get_key(tt_file, bucket_name=bucket)
        shp_obj = s3.get_key(shp_file, bucket_name=bucket)

        tt_df = pd.read_csv(io.BytesIO(tt_obj.get()['Body'].read()))
        shp_df = pd.read_csv(io.BytesIO(shp_obj.get()['Body'].read()))
        
        return {
            'tt_data': tt_df.to_json(orient='split'),
            'shp_data': shp_df.to_json(orient='split')
        }

    @task
    def transform_data(raw_data, year: int, month: int):
        tt_df = pd.read_json(raw_data['tt_data'], orient='split')
        shp_df = pd.read_json(raw_data['shp_data'], orient='split')

        # Preprocessing
        tt_df['measurement_tstamp'] = pd.to_datetime(tt_df['measurement_tstamp'])
        tt_df['wday'] = tt_df['measurement_tstamp'].dt.weekday + 1
        tt_df['hour'] = tt_df['measurement_tstamp'].dt.hour

        def get_period(row):
            if 6 <= row['hour'] < 10 and 1 < row['wday'] < 7:
                return 'amp'
            elif 10 <= row['hour'] < 16 and 1 < row['wday'] < 7:
                return 'mid'
            elif 16 <= row['hour'] < 20 and 1 < row['wday'] < 7:
                return 'pmp'
            elif 6 <= row['hour'] < 20:
                return 'we'
            else:
                return None

        tt_df['period'] = tt_df.apply(get_period, axis=1)
        tt_df = tt_df.dropna(subset=['period'])

        # Calculate percentiles
        result = tt_df.groupby(['tmc_code', 'period']).agg(
            tt50=('travel_time_seconds', lambda x: np.quantile(x, 0.5)),
            tt85=('travel_time_seconds', lambda x: np.quantile(x, 0.85)),
            tt95=('travel_time_seconds', lambda x: np.quantile(x, 0.95)),
        ).reset_index()

        # Calculate TTI and PTI
        result['tti'] = result['tt85'] / result['tt50']
        result['pti'] = result['tt95'] / result['tt50']

        # Merge with shapefile
        df_final = result.merge(shp_df, on='tmc_code', how='left')

        # Filter for HONOLULU only
        df_final = df_final[df_final['county'] == 'HONOLULU']

        # Identify Top 10 by TTI and PTI
        top_tti = df_final.sort_values('tti', ascending=False).head(10)
        top_pti = df_final.sort_values('pti', ascending=False).head(10)

        return {
            'top_tti': top_tti.to_json(orient='split'),
            'top_pti': top_pti.to_json(orient='split'),
            'year': year,
            'month': month
        }

    @task
    def load_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        top_tti = pd.read_json(transformed_data['top_tti'], orient='split')
        top_pti = pd.read_json(transformed_data['top_pti'], orient='split')

        top_tti['analysis_period'] = f"{transformed_data['year']}-{transformed_data['month']:02d}"
        top_pti['analysis_period'] = f"{transformed_data['year']}-{transformed_data['month']:02d}"

        top_tti.to_sql('top_10_tti', con=engine, if_exists='append', index=False)
        top_pti.to_sql('top_10_pti', con=engine, if_exists='append', index=False)

    # Execution Order
    from airflow.utils.dates import days_ago
    execution_date = datetime.now()

    year = execution_date.year
    month = execution_date.month

    raw_data = extract_data(year, month)
    transformed = transform_data(raw_data, year, month)
    load_data(transformed)
```
