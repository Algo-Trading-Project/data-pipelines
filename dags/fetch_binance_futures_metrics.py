from airflow import DAG
from operators.get_binance_futures_metrics_data_operator import GetBinanceFuturesMetricsOperator

from datetime import timedelta
import pendulum

schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_futures_metrics',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    futures_metrics_to_duck_db = GetBinanceFuturesMetricsOperator(
        task_id = 'futures_metrics_to_duck_db'
    )

    futures_metrics_to_duck_db
