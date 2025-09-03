from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

import duckdb
import pandas as pd
import fsspec

def agg_futures_ohlcv_data_1d_duckdb(**context):
    date = context['logical_date']
    date = pd.to_datetime(date)
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    print('Starting aggregation for date:', date.strftime('%Y-%m-%d'))
    print('Collecting data for previous date:', prev_date)
    print()

    RAW_FUTURES_OHLCV = '~/LocalData/data/futures_ohlcv_data/raw'
    AGG_FUTURES_OHLCV = '~/LocalData/data/futures_ohlcv_data/agg'

    # Setup filesystem
    fs = fsspec.filesystem('file') 

    # Expand paths
    input_path = fs.expand_path(f"{RAW_FUTURES_OHLCV}/symbol_id=*/date={prev_date}/*.parquet")
    output_path = fs.expand_path(f"{AGG_FUTURES_OHLCV}/")
    print('Input path:', input_glob)
    print('Output path:', output_path)
    print()
    # DuckDB SQL query for 1D aggregation (right-labeled)
    query = f"""
        WITH futures_ohlcv_agg_1d AS (
            SELECT
                date_trunc('day', time_period_end) AS date,
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
            FROM read_parquet('{input_path}')
            GROUP BY date_trunc('day', time_period_end), asset_id_base, asset_id_quote, exchange_id
        )
        COPY (
            SELECT * FROM futures_ohlcv_agg_1d
        )
        TO '{output_path}' (
            FORMAT PARQUET,
            COMPRESSION 'SNAPPY',
            PARTITION_BY (symbol_id, date),
            WRITE_PARTITION_COLUMNS true,
            OVERWRITE
        );
    """
    duckdb.sql(query)

# Define Airflow DAG
with DAG(
    dag_id="agg_futures_ohlcv_data_1d",
    catchup=False,
) as dag:

    wait_for_fetch = ExternalTaskSensor(
        task_id="wait_for_fetch_futures_ohlcv_data",
        external_dag_id="fetch_binance_futures_ohlcv_data_1m",
        external_task_id="finish_futures_ohlcv_data",
        mode="reschedule",
        timeout=60 * 60,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    aggregate = PythonOperator(
        task_id="aggregate_futures_ohlcv",
        python_callable=agg_futures_ohlcv_data_1d_duckdb,
        op_kwargs={"date": "{{ ds }}"},
    )
    
    finish = EmptyOperator(task_id="finish")

    wait_for_fetch >> aggregate >> finish