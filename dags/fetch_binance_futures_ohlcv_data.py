from airflow import DAG
from operators.get_binance_futures_ohlcv_data_operator import GetBinanceFuturesOHLCVDataOperator
from airflow.datasets import Dataset
# Dummy Operator for testing
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from datasets import RAW_FUTURES_OHLCV
import pendulum

schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_futures_ohlcv_data_1m',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    futures_price_data_1m_to_duck_db = GetBinanceFuturesOHLCVDataOperator(
        task_id = 'futures_price_data_1m_to_duck_db'
    )
    finish = DummyOperator(task_id = 'finish_futures_ohlcv_data', outlets=[RAW_FUTURES_OHLCV]) # This dataset will be updated after the task runs

    futures_price_data_1m_to_duck_db >> finish
