## Project Overview

The purpose of this project was to create a fully automated ETL pipeline using using Airflow and Snowflake to track if congestion (measured by TTI) worsened or improved over the last 10 years, updating yearly.
TTI is 85th percentile / 50th percentile of travel time. 

## Data Sources

The primary dataset used for this analysis is the National Performance Management Research Data Set (NPMRDS). It provides detailed travel time data at 5-minute intervals. 

![link3_data_head](https://github.com/user-attachments/assets/1c8bcfc0-ee93-45cf-8836-b937906bf884)

### 1. ETL Pipeline (Monthly)
-Extract: Read yearly NPMRDS data from S3.

-Transform: 
For each year, compute TTI trends by time period (AM, MID, PM, Weekend). Compare yearly TTI to previous years.

-Load: Save yearly congestion summaries into Snowflake (tables: tti_summary, tti_trends).

### 2. Airflow DAG Script
```python
# pip install apache-airflow[snowflake] snowflake-connector-python pandas numpy
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import numpy as np
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='yearly_congestion_trend_analysis',
    default_args=default_args,
    schedule_interval='@yearly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['congestion', 'tti', 'snowflake']
) as dag:

    @task
    def extract_data(start_year: int, end_year: int):
        all_years_summary = []

        for year in range(start_year, end_year + 1):
            print(f"Processing {year}...")
            try:
                tt_file = f"/path/to/data/HITTAV{year}.csv"
                shp_file = f"/path/to/data/TMC_Identification{year}.csv"
                tt = pd.read_csv(tt_file)
                shp = pd.read_csv(shp_file)
            except FileNotFoundError:
                print(f"Data for {year} not found. Skipping.")
                continue

            # Preprocessing
            tt['measurement_tstamp'] = pd.to_datetime(tt['measurement_tstamp'])
            tt['wday'] = tt['measurement_tstamp'].dt.weekday + 1
            tt['hour'] = tt['measurement_tstamp'].dt.hour

            time_periods = {
                'amp': lambda df: (df['wday'] > 1) & (df['wday'] < 7) & (df['hour'] >= 6) & (df['hour'] < 10),
                'mid': lambda df: (df['wday'] > 1) & (df['wday'] < 7) & (df['hour'] >= 10) & (df['hour'] < 16),
                'pmp': lambda df: (df['wday'] > 1) & (df['wday'] < 7) & (df['hour'] >= 16) & (df['hour'] < 20),
                'we': lambda df: ~((df['wday'] > 1) & (df['wday'] < 7)) & (df['hour'] >= 6) & (df['hour'] < 20)
            }

            def summarize(df):
                return df.groupby('tmc_code').agg({
                    'travel_time_seconds': [('ttq50', lambda x: np.quantile(x, 0.5)),
                                            ('ttq80', lambda x: np.quantile(x, 0.8))]
                }).droplevel(0, axis=1).reset_index()

            lottr_values = {}
            for period, condition in time_periods.items():
                temp_df = tt[condition(tt)]
                if not temp_df.empty:
                    summary = summarize(temp_df)
                    summary[f'lottr_{period}'] = summary['ttq80'] / summary['ttq50']
                    lottr_values[period] = summary[['tmc_code', f'lottr_{period}']]

            # Merge period LOTTRs
            if lottr_values:
                df = lottr_values[list(lottr_values.keys())[0]]
                for period in list(lottr_values.keys())[1:]:
                    df = df.merge(lottr_values[period], on='tmc_code', how='left')
                df = df.fillna(0)
            else:
                continue

            df = df.merge(shp, on='tmc_code', how='left')
            df_filtered = df[(df['county'] == 'HONOLULU')]

            # Yearly summary
            lottr_means = {period: df_filtered[f'lottr_{period}'].mean() for period in time_periods}
            lottr_means['year'] = year
            all_years_summary.append(lottr_means)

        return pd.DataFrame(all_years_summary).to_json(orient='split')

    @task
    def load_to_snowflake(summary_json):
        df = pd.read_json(summary_json, orient='split')
        hook = SnowflakeHook(snowflake_conn_id='your_snowflake_conn')
        conn = hook.get_sqlalchemy_engine()

        # Write yearly TTI summary
        df.to_sql('tti_summary', con=conn, if_exists='append', index=False)

        # Compute trend (improve/worsen)
        df['amp_change'] = df['amp'].pct_change().fillna(0)
        df['mid_change'] = df['mid'].pct_change().fillna(0)
        df['pmp_change'] = df['pmp'].pct_change().fillna(0)
        df['we_change'] = df['we'].pct_change().fillna(0)

        trend_df = df[['year', 'amp_change', 'mid_change', 'pmp_change', 'we_change']]
        trend_df.to_sql('tti_trends', con=conn, if_exists='append', index=False)

    # DAG execution flow
    raw_summary = extract_data(2015, datetime.now().year)
    load_to_snowflake(raw_summary)
```
