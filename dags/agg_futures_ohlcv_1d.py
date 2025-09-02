from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
# from datasets import RAW_FUTURES_OHLCV, AGG_FUTURES_OHLCV

import duckdb
import pandas as pd
import fsspec

RAW_FUTURES_OHLCV = Dataset("~/LocalData/data/futures_ohlcv_data/raw")
AGG_FUTURES_OHLCV = Dataset("~/LocalData/data/futures_ohlcv_data/agg")

def agg_futures_ohlcv_data_1d_duckdb(date: str):
    date = pd.to_datetime(date)
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    date_str = date.strftime('%Y-%m-%d')

    # Setup filesystem
    fs = fsspec.filesystem('file') if RAW_FUTURES_OHLCV.uri.startswith('~') or RAW_FUTURES_OHLCV.uri.startswith('/') else fsspec.filesystem('s3')

    # Expand paths
    input_path = fs.expand_path(f"{RAW_FUTURES_OHLCV.uri}/symbol_id=*/date={prev_date}/*.parquet")
    output_path = fs.expand_path(f"{AGG_FUTURES_OHLCV.uri}")
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
    print('Aggregation complete for date:', date_str)

# Define Airflow DAG
with DAG(
    dag_id="agg_futures_ohlcv_data_1d",
    schedule=[RAW_FUTURES_OHLCV],
    catchup=False,
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate_futures_ohlcv",
        python_callable=agg_futures_ohlcv_data_1d_duckdb,
        op_kwargs={"date": "{{ ds }}"},
    )
    finish = EmptyOperator(task_id="finish", outlets=[AGG_FUTURES_OHLCV])

    aggregate >> finish