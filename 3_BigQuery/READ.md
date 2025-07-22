## Project Overview

The purpose of this project was to create a fully automated ETL pipeline using using Airflow + BigQuery to identify top 10 congested roads (based on TTI over the years), calculate the average TTI change from 2015 to 2024, and 
determine how often TTI exceeds 1.5 (unreliable travel times). TTI is 85th percentile / 50th percentile of travel time. 

## Data Sources

The primary dataset used for this analysis is the National Performance Management Research Data Set (NPMRDS). It provides detailed travel time data at 5-minute intervals. 

![link3_data_head](https://github.com/user-attachments/assets/1c8bcfc0-ee93-45cf-8836-b937906bf884)

### 1. ETL Pipeline (Monthly)
-Extract: Read yearly NPMRDS data

-Transform: 
Compute TTI (85th percentile / 50th percentile). Calculate yearly TTI change for top 10 roads. Compute exceedance rates (TTI > 1.5).

-Load: Store results in BigQuery tables: (tti_summary, tti_exceedance, and tti_top10_trends).
-Schedule: Run yearly (e.g., January each year for previous year).

### 2. Airflow DAG Script
```python
# pip install apache-airflow[gcp] pandas numpy google-cloud-bigquery
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import numpy as np
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='yearly_tti_trend_analysis_bigquery',
    default_args=default_args,
    schedule_interval='@yearly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['tti', 'bigquery', 'congestion']
) as dag:

    @task
    def extract_and_transform(start_year: int, end_year: int):

        all_data = []

        for year in range(start_year, end_year + 1):
            try:
                tt_file = f"/path/to/data/HITTAV{year}.csv"
                shp_file = f"/path/to/data/TMC_Identification{year}.csv"
                tt = pd.read_csv(tt_file)
                shp = pd.read_csv(shp_file)
            except FileNotFoundError:
                continue

            tt['measurement_tstamp'] = pd.to_datetime(tt['measurement_tstamp'])
            tt['wday'] = tt['measurement_tstamp'].dt.weekday + 1
            tt['hour'] = tt['measurement_tstamp'].dt.hour

            # Define time periods
            periods = {
                'amp': lambda df: (df['wday'] > 1) & (df['wday'] < 7) & (df['hour'] >= 6) & (df['hour'] < 10),
                'mid': lambda df: (df['wday'] > 1) & (df['wday'] < 7) & (df['hour'] >= 10) & (df['hour'] < 16),
                'pmp': lambda df: (df['wday'] > 1) & (df['wday'] < 7) & (df['hour'] >= 16) & (df['hour'] < 20),
                'we': lambda df: ~((df['wday'] > 1) & (df['wday'] < 7)) & (df['hour'] >= 6) & (df['hour'] < 20)
            }

            tti_records = []

            for period, condition in periods.items():
                temp = tt[condition(tt)]

                grouped = temp.groupby('tmc_code')['travel_time_seconds'].agg([
                    ('tt50', lambda x: np.quantile(x, 0.5)),
                    ('tt85', lambda x: np.quantile(x, 0.85)),
                ]).reset_index()

                grouped['tti'] = grouped['tt85'] / grouped['tt50']
                grouped['period'] = period
                grouped['year'] = year

                tti_records.append(grouped)

            year_df = pd.concat(tti_records)
            year_df = year_df.merge(shp, on='tmc_code', how='left')
            year_df = year_df[year_df['county'] == 'HONOLULU']

            all_data.append(year_df)

        df_all = pd.concat(all_data)

        return df_all.to_json(orient='split')

    @task
    def load_to_bigquery(tti_json):
        df = pd.read_json(tti_json, orient='split')
        client = bigquery.Client()
        dataset = 'your_dataset'

        # Write yearly summary
        df[['year', 'tmc_code', 'period', 'tti']].to_gbq(f'{dataset}.tti_summary', project_id='your_project', if_exists='append')

        # Compute Top 10 TTI congested roads (avg across all years/periods)
        top10 = df.groupby('tmc_code')['tti'].mean().sort_values(ascending=False).head(10).index.tolist()
        top10_df = df[df['tmc_code'].isin(top10)]

        # Compute TTI change over years for top 10 roads
        trend_df = top10_df.groupby(['tmc_code', 'year'])['tti'].mean().reset_index()
        trend_df['tti_change'] = trend_df.groupby('tmc_code')['tti'].diff().fillna(0)

        trend_df.to_gbq(f'{dataset}.tti_top10_trends', project_id='your_project', if_exists='append')

        # Compute exceedance rates (tti > 1.5)
        df['exceed_1_5'] = df['tti'] > 1.5
        exceed_df = df.groupby(['tmc_code', 'year', 'period']).agg(
            exceed_rate=('exceed_1_5', 'mean')
        ).reset_index()

        exceed_df.to_gbq(f'{dataset}.tti_exceedance', project_id='your_project', if_exists='append')

    data = extract_and_transform(2015, 2024)
    load_to_bigquery(data)
```
