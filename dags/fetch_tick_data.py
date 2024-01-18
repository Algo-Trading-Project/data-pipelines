from airflow import DAG
from operators.get_tick_data_operator import GetTickDataOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import timedelta
import pendulum

def on_task_success(context):
    """
    Deletes existing tick data from S3.

    This function is used to clear out any existing tick data from the specified S3 bucket, 
    run after the data has been successfully loaded into Redshift.

    Returns:
        None
    """

    s3_hook = S3Hook(aws_conn_id = 's3_conn')

    # Get keys for tick data stored in S3
    keys_to_delete = s3_hook.list_keys(
        bucket_name = 'project-poseidon-data',
        prefix = 'eth_data/tick_data'
    )

    # Delete tick data from S3
    s3_hook.delete_objects(
        bucket = 'project-poseidon-data', 
        keys = keys_to_delete
    )

schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(
    year = 2023,
    month = 8,
    day = 15,
    tz = 'America/Los_Angeles'
)

with DAG(
    dag_id = 'fetch_tick_data_1h',
    start_date = start_date,
    schedule_interval = schedule_interval,
    max_active_runs =  1,
    catchup = False
) as dag:
    
    tick_data_1h_cols = [
        'symbol_id', 'time_exchange', 'time_coinapi', 'uuid',
        'price', 'size', 'taker_side', 'asset_id_base', 
        'asset_id_quote', 'exchange_id', 'id_trade', 
        'id_order_maker', 'id_order_taker'
    ]

    upsert_keys = [
        'exchange_id', 'asset_id_base', 'asset_id_quote',
        'time_exchange', 'time_coinapi', 'uuid', 'taker_side'
    ]
    
    tick_data_to_s3 = GetTickDataOperator(
        task_id = 'tick_data_to_s3'
    )
    
    s3_tick_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_tick_data_to_redshift',
        trigger_rule = 'all_done',
        schema = 'coinapi',
        table = 'tick_data',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/tick_data',
        redshift_conn_id = 'token_price_database_conn',
        aws_conn_id = 's3_conn',
        method = 'UPSERT',
        upsert_keys = upsert_keys,
        copy_options = ["json 'auto'", "TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS'"],
        column_list = tick_data_1h_cols,
        on_sucess_callback = on_task_success
    )

    tick_data_to_s3 >> s3_tick_data_to_redshift
