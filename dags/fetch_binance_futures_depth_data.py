from airflow import DAG
from operators.get_binance_futures_depth_data_operator import GetBinanceFuturesDepthDataOperator

from datetime import timedelta
import pendulum

start_date = pendulum.datetime(
    year = 2020,
    month = 1,
    day = 1,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_futures_depth_data',
    start_date = start_date,
    schedule_interval = '@daily',
    max_active_runs =  1,
    catchup = False
) as dag:
    
    futures_depth_data_to_duck_db = GetBinanceFuturesDepthDataOperator(
        task_id = 'futures_depth_data_to_duck_db'
    )

    futures_depth_data_to_duck_db
