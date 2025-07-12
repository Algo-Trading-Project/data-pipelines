from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datasets import RAW_FUTURES_OHLCV, AGG_FUTURES_OHLCV
import duckdb
import pandas as pd
import fsspec
from urllib.parse import urljoin
from datetime import datetime

def agg_futures_ohlcv_data_1d(date: str):
    # Convert Airflow date string
    date_str = pd.to_datetime(date).strftime('%Y-%m-%d')

    # Setup fsspec filesystem
    fs = fsspec.filesystem('file') if RAW_FUTURES_OHLCV.uri.startswith('~') or RAW_FUTURES_OHLCV.uri.startswith('/') else fsspec.filesystem('s3')

    # Normalize base URIs
    input_root = fs.expand_path(RAW_FUTURES_OHLCV.uri)
    output_root = fs.expand_path(AGG_FUTURES_OHLCV.uri)

    # List all symbol partitions
    symbol_dirs = fs.glob(f"{input_root}/symbol_id=*")

    for symbol_dir in symbol_dirs:
        # Build full daily path
        date_partition_path = f"{symbol_dir}/time_period_open={date_str}"

        # List all Parquet files for the symbol on that day
        parquet_files = fs.glob(f"{date_partition_path}/*.parquet")

        if not parquet_files:
            continue

        # Load all files into one DataFrame
        df = pd.concat([
            pd.read_parquet(file, filesystem=fs)
            for file in parquet_files
        ])

        # Run daily downsampling in Python
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp").sort_index()
        df_agg = df.resample("1D", label="right", closed="left").agg({
            'asset_id_base': 'last',
            'asset_id_quote': 'last',
            'exchange_id': 'last',
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'trades': 'sum'
        }).dropna().reset_index()

        if df_agg.empty:
            continue

        # Preserve partitioning in output path
        symbol_id = df_agg['asset_id_base'][0] + "_" + df_agg['asset_id_quote'][0] + "_" + df_agg['exchange_id'][0]
        output_path = f"{output_root}/symbol_id={symbol_id}/time_period_open={date_str}/agg.parquet"

        # Save back to target partition
        with fs.open(output_path, 'wb') as f:
            df_agg.to_parquet(f, index=False, compression='snappy')

# Define DAG
with DAG(
    dag_id="agg_futures_ohlcv_data_1d",
    schedule=[RAW_FUTURES_OHLCV],
    catchup=False
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate_futures_ohlcv",
        python_callable=agg_futures_ohlcv_data_1d,
        op_kwargs={"date": "{{ ds }}"},
    )
    finish = DummyOperator(task_id="finish", outlets=[AGG_FUTURES_OHLCV])

    aggregate >> finish