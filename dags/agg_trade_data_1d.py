from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

# from datasets import RAW_SPOT_TRADES, AGG_SPOT_TRADES

import duckdb
import pandas as pd
import fsspec

RAW_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/raw")
AGG_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/agg")

def agg_trade_data_1d(**context):
    date = context['logical_date'] if 'logical_date' in context else context['data_interval_end'].strftime('%Y-%m-%d')
    print('Starting aggregation for date:', date)
    date = pd.to_datetime(date)
    prev_date = (date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    date_str = date.strftime('%Y-%m-%d')
    
    # Setup fsspec filesystem
    fs = fsspec.filesystem('file') if RAW_FUTURES_TRADES.uri.startswith('~') or RAW_FUTURES_TRADES.uri.startswith('/') else fsspec.filesystem('s3')

    # Normalize base URIs
    input_root = fs.expand_path(RAW_SPOT_TRADES.uri)
    output_root = fs.expand_path(AGG_SPOT_TRADES.uri)
    
    input_path = f"{input_root}/symbol_id=*/date={prev_date}/*.parquet"
    output_path = f"{output_root}/"

    query = f"""
    WITH spot_trade_data_agg_1d AS (
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
        SELECT * FROM spot_trade_data_agg_1d
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

with DAG(
    dag_id="agg_spot_trade_data_1d",
    schedule=[RAW_SPOT_TRADES],
    catchup=False
) as dag:
    make = PythonOperator(
        task_id="build_spot_trade_features",
        python_callable=agg_trade_data_1d
    )
    finish = EmptyOperator(task_id="finish", outlets=[AGG_SPOT_TRADES])