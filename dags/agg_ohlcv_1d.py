from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
# from datasets import RAW_SPOT_OHLCV, AGG_SPOT_OHLCV

import duckdb
import fsspec
import pandas as pd
from datetime import datetime

RAW_SPOT_OHLCV   = Dataset("~/LocalData/data/ohlcv_data/raw")
AGG_SPOT_OHLCV   = Dataset("~/LocalData/data/ohlcv_data/agg")

def agg_spot_ohlcv_data_1d(**context):
    date = context['logical_date'] if 'logical_date' in context else context['data_interval_end'].strftime('%Y-%m-%d')
    print('Starting aggregation for date:', date)
    date = pd.to_datetime(date)
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    date_str = date.strftime('%Y-%m-%d')

    # Determine FS backend
    fs = fsspec.filesystem('file') if RAW_SPOT_OHLCV.uri.startswith('~') or RAW_SPOT_OHLCV.uri.startswith('/') else fsspec.filesystem('s3')

    # Expanded input/output paths
    input_glob = f"{RAW_SPOT_OHLCV.uri}/symbol_id=*/date={prev_date}/*.parquet"
    output_path = fs.expand_path(AGG_SPOT_OHLCV.uri)[0]
    print('Input path:', input_glob)
    print('Output path:', output_path)
    print()

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
                FROM read_parquet('{input_glob}', hive_partitioning=true)
                GROUP BY date_trunc('day', time_period_end), asset_id_base, asset_id_quote, exchange_id
            )
            SELECT * FROM ohlcv_agg_1d
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

# Define DAG
with DAG(
    dag_id="agg_spot_ohlcv_data_1d",
    start_date=datetime(2018, 1, 1),
    schedule=[RAW_SPOT_OHLCV],
    catchup=False,
    max_active_runs=1,
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate_spot_ohlcv",
        python_callable=agg_spot_ohlcv_data_1d,
        # op_kwargs={"date": "{{ ds }}"},
    )
    finish = EmptyOperator(task_id="finish", outlets=[AGG_SPOT_OHLCV])

    aggregate >> finish