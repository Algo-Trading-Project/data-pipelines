from airflow import DAG
from operators.get_binance_futures_ohlcv_data_1d_operator import GetBinanceFuturesOHLCVDataDailyOperator
from airflow.datasets import Dataset
# Dummy Operator for testing
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
from datasets import RAW_FUTURES_OHLCV
import pendulum

start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
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
    finish = EmptyOperator(task_id = 'finish_futures_ohlcv_data', outlets=[RAW_FUTURES_OHLCV])
    futures_price_data_1m_to_duck_db >> finish
