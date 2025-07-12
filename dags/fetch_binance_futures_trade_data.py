from airflow import DAG
from operators.get_binance_futures_trade_data_operator import GetBinanceFuturesTradeDataOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from datasets import RAW_FUTURES_TRADES
import pendulum

schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_futures_trade_data',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    futures_trade_data_to_duck_db = GetBinanceFuturesTradeDataOperator(
        task_id = 'futures_trade_data_to_duck_db'
    )
    finish = DummyOperator(task_id = 'finish', outlets = [RAW_FUTURES_TRADES])

    futures_trade_data_to_duck_db >> finish