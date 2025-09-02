from airflow import DAG
from operators.get_binance_trade_data_1d_operator import GetBinanceTradeDataDailyOperator
from airflow.operators.empty import EmptyOperator
# from datasets import RAW_SPOT_TRADES
from airflow.datasets import Dataset
from datetime import timedelta
import pendulum

RAW_SPOT_TRADES   = Dataset("~/LocalData/data/trade_data/raw")

start_date = pendulum.datetime(
    year = 2018,
    month = 1,
    day = 1,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_trade_data',
    start_date = start_date,
    schedule = '@daily',
    max_active_runs =  1,
    catchup = False
) as dag:
    
    trade_data_to_duck_db = GetBinanceTradeDataDailyOperator(
        task_id = 'trade_data_to_duck_db'
    )
    finish = EmptyOperator(task_id = 'finish', outlets = [RAW_SPOT_TRADES])

    trade_data_to_duck_db >> finish
