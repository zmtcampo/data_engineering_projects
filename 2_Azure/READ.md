## Project Overview

The purpose of this project was to create a fully automated ETL pipeline using using Airflow + Azure (Data Lake + Azure SQL / Synapse), running monthly to generate the Top 10 TTI (Travel Time Index), PTI (Planning Time Index), and Variability.
TTI is 85th percentile / 50th percentile of travel time. PTI is 95th percentile / 50th percentile of travel time.

## Data Sources

The primary dataset used for this analysis is the National Performance Management Research Data Set (NPMRDS). It provides detailed travel time data at 5-minute intervals. 

![link3_data_head](https://github.com/user-attachments/assets/1c8bcfc0-ee93-45cf-8836-b937906bf884)

### 1. ETL Pipeline (Monthly)
-Extract: Read raw CSVs from Azure Blob Storage

-Transform: Calculate TTI, PTI, Severity, and Variability. Identify top 10 severe TMCs per year and identify most variable TMCs over 10 years.

-Load: Write to Azure SQL / Synapse tables (tti_severity_summary, tti_variability_summary)

### 2. Airflow DAG Script
```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='yearly_congestion_severity_variability_azure',
    default_args=default_args,
    schedule_interval='@yearly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['tti', 'azure', 'congestion']
) as dag:

    @task
    def process_and_load(start_year: int, end_year: int):

        severity_records = []
        variability_records = []

        for year in range(start_year, end_year + 1):
            try:
                tt_file = f"/mnt/data/HITTAV{year}.csv"
                shp_file = f"/mnt/data/TMC_Identification{year}.csv"
                tt = pd.read_csv(tt_file)
                shp = pd.read_csv(shp_file)
            except FileNotFoundError:
                continue

            tt['measurement_tstamp'] = pd.to_datetime(tt['measurement_tstamp'])
            tt['wday'] = tt['measurement_tstamp'].dt.weekday + 1
            tt['hour'] = tt['measurement_tstamp'].dt.hour

            def compute_indices(df):
                grouped = df.groupby('tmc_code').agg({
                    'travel_time_seconds': [
                        ('tt50', lambda x: np.quantile(x, 0.5)),
                        ('tt85', lambda x: np.quantile(x, 0.85)),
                        ('tt95', lambda x: np.quantile(x, 0.95))
                    ]
                }).droplevel(0, axis=1).reset_index()

                grouped['tti'] = grouped['tt85'] / grouped['tt50']
                grouped['pti'] = grouped['tt95'] / grouped['tt50']
                grouped['severity'] = (grouped['tti'] + grouped['pti']) / 2
                grouped['variability'] = grouped['pti'] / grouped['tti']
                return grouped

            indices_df = compute_indices(tt)
            indices_df['year'] = year
            indices_df = indices_df.merge(shp, on='tmc_code', how='left')
            indices_df = indices_df[indices_df['county'] == 'HONOLULU']

            # Top 10 severe roads by year
            top10_severe = indices_df.nlargest(10, 'severity')
            severity_records.append(top10_severe[['tmc_code', 'year', 'severity']])

            # Store all variability for 10-year aggregation
            variability_records.append(indices_df[['tmc_code', 'year', 'variability']])

        # Combine all years
        severity_df = pd.concat(severity_records)
        variability_df = pd.concat(variability_records)

        # Most variable TMCs over 10 years (average variability)
        most_variable = variability_df.groupby('tmc_code')['variability'].mean().reset_index()
        most_variable = most_variable.sort_values(by='variability', ascending=False).head(10)
        most_variable['years'] = f'{start_year}-{end_year}'

        # Write to Azure SQL Database or Synapse
        conn_str = 'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
        engine = create_engine(conn_str.format(
            username='your_username',
            password='your_password',
            server='your_server.database.windows.net',
            database='your_database'
        ))

        severity_df.to_sql('tti_severity_summary', con=engine, if_exists='append', index=False)
        most_variable.to_sql('tti_variability_summary', con=engine, if_exists='replace', index=False)

    process_and_load(2015, 2024)
```
