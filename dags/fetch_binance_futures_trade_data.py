from airflow import DAG
from operators.get_binance_futures_trade_data_1d_operator import GetBinanceFuturesTradeDataDailyOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
from datasets import RAW_FUTURES_TRADES
import pendulum

start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
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
    finish = EmptyOperator(task_id = 'finish', outlets = [RAW_FUTURES_TRADES])

    futures_trade_data_to_duck_db >> finish