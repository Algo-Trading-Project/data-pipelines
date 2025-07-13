from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datasets import RAW_SPOT_OHLCV, AGG_SPOT_OHLCV

import duckdb
import fsspec
import pandas as pd
from datetime import datetime

def agg_spot_ohlcv_data_1d_duckdb(date: str):
    date = pd.to_datetime(date)
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    date_str = date.strftime('%Y-%m-%d')

    # Determine FS backend
    fs = fsspec.filesystem('file') if RAW_SPOT_OHLCV.uri.startswith('~') or RAW_SPOT_OHLCV.uri.startswith('/') else fsspec.filesystem('s3')

    # Expanded input/output paths
    input_path = fs.expand_path(f"{RAW_SPOT_OHLCV.uri}/symbol_id=*/date={prev_date}/*.parquet")
    output_path = fs.expand_path(f"{AGG_SPOT_OHLCV.uri}")

    # DuckDB query (right-labeled)
    query = f"""
        WITH ohlcv_agg_1d AS (
            SELECT
                date_trunc('day', time_period_end) + INTERVAL 1 day AS date,
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
            GROUP BY date_trunc('day', time_period_end) + INTERVAL 1 day, asset_id_base, asset_id_quote, exchange_id
        )
        COPY (
            SELECT * FROM ohlcv_agg_1d
        )
        TO '{output_path}' (
            FORMAT PARQUET,
            COMPRESSION 'SNAPPY',
            PARTITION_BY (symbol_id, date)
        );
    """

    duckdb.sql(query)

# Define DAG
with DAG(
    dag_id="agg_spot_ohlcv_data_1d_duckdb",
    schedule=[RAW_SPOT_OHLCV],
    catchup=False
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate_spot_ohlcv",
        python_callable=agg_spot_ohlcv_data_1d_duckdb,
        op_kwargs={"date": "{{ ds }}"},
    )
    finish = EmptyOperator(task_id="finish", outlets=[AGG_SPOT_OHLCV])

    aggregate >> finish