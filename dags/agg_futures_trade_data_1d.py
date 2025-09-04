# Refactored DAG: Aggregate Futures Trades with Hive-Partitioned COPY

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

import duckdb
import pandas as pd
import fsspec
import pendulum

def agg_futures_trade_data_1d(**context):
    date = context['logical_date']
    date = pd.to_datetime(date)  
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    RAW_FUTURES_TRADES = '~/LocalData/data/futures_trade_data/raw'
    AGG_FUTURES_TRADES = '~/LocalData/data/futures_trade_data/agg'

    # Setup filesystem
    fs = fsspec.filesystem('file') 

    # Expand paths
    input_path = fs.expand_path(f"{RAW_FUTURES_TRADES}/symbol_id=*/date={prev_date}/*.parquet")
    output_path = fs.expand_path(f"{AGG_FUTURES_TRADES}/")

    query = f"""
    WITH futures_trade_data_agg_1d AS (
        SELECT
            date_trunc('day', timestamp) AS date,
            asset_id_base,
            asset_id_quote,
            exchange_id,
            asset_id_base || '_' || asset_id_quote || '_' || exchange_id AS symbol_id,
            SUM(quote_quantity) AS total_dollar_volume,
            SUM(CASE WHEN side = 'buy' THEN quote_quantity ELSE 0 END) AS total_buy_dollar_volume,
            SUM(CASE WHEN side = 'sell' THEN quote_quantity ELSE 0 END) AS total_sell_dollar_volume,
            SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END) AS num_buys,
            SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS num_sells,
            MEAN(quote_quantity) AS avg_dollar_volume,
            MEAN(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END) AS avg_buy_dollar_volume,
            MEAN(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END) AS avg_sell_dollar_volume,
            STDDEV_POP(quote_quantity) AS std_dollar_volume,
            STDDEV_POP(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END) AS std_buy_dollar_volume,
            STDDEV_POP(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END) AS std_sell_dollar_volume,
            SKEWNESS(quote_quantity) AS skew_dollar_volume,
            SKEWNESS(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END) AS skew_buy_dollar_volume,
            SKEWNESS(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END) AS skew_sell_dollar_volume,
            KURTOSIS_POP(quote_quantity) AS kurtosis_dollar_volume,
            KURTOSIS_POP(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END) AS kurtosis_buy_dollar_volume,
            KURTOSIS_POP(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END) AS kurtosis_sell_dollar_volume,
            QUANTILE_CONT(quote_quantity, 0.1) AS "10th_percentile_dollar_volume",
            QUANTILE_CONT(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END, 0.1) AS "10th_percentile_buy_dollar_volume",
            QUANTILE_CONT(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END, 0.1) AS "10th_percentile_sell_dollar_volume",
            MEDIAN(quote_quantity) AS median_dollar_volume,
            MEDIAN(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END) AS median_buy_dollar_volume,
            MEDIAN(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END) AS median_sell_dollar_volume,
            QUANTILE_CONT(quote_quantity, 0.9) AS "90th_percentile_dollar_volume",
            QUANTILE_CONT(CASE WHEN side = 'buy' THEN quote_quantity ELSE NULL END, 0.9) AS "90th_percentile_buy_dollar_volume",
            QUANTILE_CONT(CASE WHEN side = 'sell' THEN quote_quantity ELSE NULL END, 0.9) AS "90th_percentile_sell_dollar_volume"
        FROM read_parquet('{input_path}')
        GROUP BY date_trunc('day', timestamp), asset_id_base, asset_id_quote, exchange_id
    )
    COPY (
        SELECT * FROM futures_trade_data_agg_1d
    )
    TO '{output_path}' (
        FORMAT PARQUET,
        COMPRESSION 'SNAPPY',
        PARTITION_BY (symbol_id, date),
        WRITE_PARTITION_COLUMNS true,
    );
    """
    duckdb.sql(query)

start_date = pendulum.datetime(
    year=2020,
    month=1,
    day=1,
    tz='America/Los_Angeles'
)

with DAG(
    dag_id="agg_futures_trade_data_1d",
    start_date=start_date,
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    wait_for_fetch = ExternalTaskSensor(
        task_id='wait_for_fetch_futures_trade_1d',
        external_dag_id='fetch_binance_futures_trade_data',
        external_task_id='finish_futures_trade_data',
        mode='reschedule',
        poke_interval=60, # 1 minute
        timeout=60 * 60, # 1 hour
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )
        
    aggregate = PythonOperator(
        task_id="build_futures_trade_features",
        python_callable=agg_futures_trade_data_1d
    )

    finish = EmptyOperator(task_id="finish")

    wait_for_fetch >> aggregate >> finish