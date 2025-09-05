from airflow import DAG
from operators.get_binance_funding_rate_data_operator import GetBinanceFundingRateOperator
import pendulum

start_date = pendulum.datetime(
    year = 2018,
    month = 1,
    day = 1,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_binance_funding_rate_data',
    start_date = start_date,
    schedule = '@daily',
    max_active_runs =  1,
    catchup = False
) as dag:
    
    funding_rate_data_to_duck_db = GetBinanceFundingRateOperator(
        task_id = 'funding_rate_data_to_duck_db'
    )

    funding_rate_data_to_duck_db
