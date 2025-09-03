from airflow import DAG
from operators.get_binance_futures_trade_data_1d_operator import GetBinanceFuturesTradeDataDailyOperator
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
    dag_id = 'fetch_binance_futures_trade_data',
    start_date = start_date,
    schedule = '@daily',
    max_active_runs =  1,
    catchup = False
) as dag:
    
    futures_trade_data_to_duck_db = GetBinanceFuturesTradeDataDailyOperator(
        task_id = 'futures_trade_data_to_duck_db'
    )
    finish = EmptyOperator(task_id = 'finish_futures_trade_data')

    futures_trade_data_to_duck_db >> finish