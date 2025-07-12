# Refactored DAG: Aggregate Futures Trades with Hive-Partitioned COPY

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datasets import RAW_FUTURES_TRADES, AGG_FUTURES_TRADES

import duckdb
import pandas as pd

def agg_futures_trade_data_1d(date):
    date = pd.to_datetime(date).strftime('%Y-%m-%d')
    input_path = f"{RAW_FUTURES_TRADES.uri}/*/time_period_open={date}/*.parquet"
    output_path = f"{AGG_FUTURES_TRADES.uri}/"

    query = f"""
    WITH futures_trade_data_agg_1d AS (
        SELECT
            date_trunc('day', timestamp) + INTERVAL '1 day' AS time_period_open,
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
        GROUP BY time_period_open, asset_id_base, asset_id_quote, exchange_id
    )
    COPY (
        SELECT * FROM futures_trade_data_agg_1d
    )
    TO '{output_path}' (
        FORMAT PARQUET,
        COMPRESSION ZSTD,
        PARTITION_BY (symbol_id, time_period_open)
    );
    """
    conn = duckdb.connect(database=':memory:')
    conn.execute(query)
    conn.close()

with DAG(
    dag_id="agg_futures_trade_data_1d",
    schedule=[RAW_FUTURES_TRADES],
    catchup=False
) as dag:
    make = PythonOperator(
        task_id="build_futures_trade_features",
        python_callable=agg_futures_trade_data_1d,
        op_kwargs={"date": "{{ ds }}"}
    )
    finish = DummyOperator(task_id="finish", outlets=[AGG_FUTURES_TRADES])