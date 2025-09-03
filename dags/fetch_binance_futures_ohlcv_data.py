from airflow import DAG
from operators.get_binance_futures_ohlcv_data_1d_operator import GetBinanceFuturesOHLCVDataDailyOperator
# Dummy Operator for testing
from airflow.operators.empty import EmptyOperator
from datetime import timedelta

import pendulum

start_date = pendulum.datetime(
    year = 2020,
    month = 1,
    day = 1,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_futures_ohlcv_data_1m',
    start_date = start_date,
    schedule = '@daily',
    max_active_runs =  1,
    catchup = False
) as dag:
    
    futures_price_data_1m_to_duck_db = GetBinanceFuturesOHLCVDataDailyOperator(
        task_id = 'futures_price_data_1m_to_duck_db'
    )
    finish = EmptyOperator(task_id = 'finish_futures_ohlcv_data')
    
    futures_price_data_1m_to_duck_db >> finish
