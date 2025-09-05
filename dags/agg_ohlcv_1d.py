from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

import duckdb
import pandas as pd
import pendulum
import os

def agg_spot_ohlcv_data_1d(**context):
    date = context['logical_date']
    date = pd.to_datetime(date)
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    # Define S3 paths
    RAW_SPOT_OHLCV = f's3://base-44-data/data/ohlcv_data/raw/symbol_id=*/date={prev_date}/*.parquet'
    AGG_SPOT_OHLCV = f's3://base-44-data/data/ohlcv_data/agg/'

    # Retrieve AWS credentials from Airflow Variables (Astronomer)
    aws_key = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret = Variable.get("AWS_SECRET_ACCESS_KEY")
    aws_region = Variable.get("AWS_DEFAULT_REGION")

    # Set environment for DuckDB (legacy auth scheme uses env vars)
    os.environ['AWS_ACCESS_KEY_ID'] = aws_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret
    os.environ['AWS_DEFAULT_REGION'] = aws_region

    # DuckDB query (left-labeled)
    query = f"""
        COPY (
            WITH ohlcv_agg_1d AS (
                SELECT
                    CAST(date_trunc('day', time_period_end) AS DATE) AS date,
                    asset_id_base,
                    asset_id_quote,
                    exchange_id,
                    asset_id_base || '_' || asset_id_quote || '_' || exchange_id AS symbol_id,
                    first(open) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    last(close) AS close,
                    sum(volume) AS volume,
                    sum(trades) AS trades
                FROM read_parquet('{RAW_SPOT_OHLCV}', hive_partitioning=true)
                GROUP BY date_trunc('day', time_period_end), asset_id_base, asset_id_quote, exchange_id
            )
            SELECT * FROM ohlcv_agg_1d
        )
        TO '{AGG_SPOT_OHLCV}' (
            FORMAT PARQUET,
            COMPRESSION 'SNAPPY',
            PARTITION_BY (symbol_id, date),
            WRITE_PARTITION_COLUMNS true,
            OVERWRITE
        );
    """
    duckdb.sql(query)

start_date = pendulum.datetime(
    year=2018,
    month=1,
    day=1,
    tz='America/Los_Angeles'
)

# Define DAG
with DAG(
    dag_id="agg_spot_ohlcv_data_1d",
    start_date=start_date,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:
    
    wait_for_fetch = ExternalTaskSensor(
        task_id='wait_for_fetch_spot_ohlcv_1d',
        external_dag_id='fetch_binance_ohlcv_data_1m',
        external_task_id='finish_ohlcv_data',
        mode='reschedule',
        poke_interval=60, # 1 minute
        timeout=60 * 60, # 1 hour
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )

    aggregate = PythonOperator(
        task_id="aggregate_spot_ohlcv",
        python_callable=agg_spot_ohlcv_data_1d
    )
    
    finish = EmptyOperator(task_id="finish")

    wait_for_fetch >> aggregate >> finish