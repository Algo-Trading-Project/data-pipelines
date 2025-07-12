from airflow import DAG
from operators.get_binance_ohlcv_data_operator import GetBinanceOHLCVDataOperator
from airflow.operators.dummy import DummyOperator
from datasets import RAW_SPOT_OHLCV
from datetime import timedelta
import pendulum

schedule_interval = timedelta(days = 1)
start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_ohlcv_data_1m',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    price_data_1m_to_duck_db = GetBinanceOHLCVDataOperator(
        task_id = 'price_data_1m_to_duck_db'
    )
    finish = DummyOperator(task_id = 'finish_ohlcv_data', outlets=[RAW_SPOT_OHLCV]) # This dataset will be updated after the task runs

    price_data_1m_to_duck_db >> finish
