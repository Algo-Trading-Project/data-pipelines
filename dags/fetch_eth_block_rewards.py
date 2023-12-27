from airflow import DAG
from airflow.models import Variable

from operators.get_block_rewards_operator import GetBlockRewardsOperator

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum
 
schedule_interval = timedelta(days = 1, hours = 1)
start_date = pendulum.datetime(year = 2022,
                               month = 4,
                               day = 14,
                               hour = 1,
                               tz = 'America/Los_Angeles')

with DAG(
    dag_id = 'fetch_eth_block_rewards',
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = False,
    max_active_runs = 1
) as dag:

    block_rewards_to_s3 = GetBlockRewardsOperator(
        task_id = 'get_eth_block_rewards'
    )

    block_rewards_table_cols = ['block_no', 'miner_address', 'block_reward']
    s3_block_rewards_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_block_rewards_to_redshift',
        trigger_rule = 'all_done',
        schema = 'eth_data',
        table = 'block_reward',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/block_rewards_data',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = block_rewards_table_cols,
        copy_options = ["json 'auto'"],
        method = 'UPSERT',
        upsert_keys = ['block_no', 'miner_address']
    )

    block_rewards_to_s3 >> s3_block_rewards_to_redshift