from airflow import DAG
from operators.get_binance_trade_data_1d_operator import GetBinanceTradeDataDailyOperator
from airflow.operators.empty import EmptyOperator
import pendulum

start_date = pendulum.datetime(
    year = 2018,
    month = 1,
    day = 1,
    hour = 0,
    minute = 30,
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
    finish = EmptyOperator(task_id = 'finish_trade_data')

    trade_data_to_duck_db >> finish
