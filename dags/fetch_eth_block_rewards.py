from airflow import DAG
from airflow.models import Variable

from operators.get_block_rewards import GetBlockRewardsOperator

from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum

def update_start_and_end_block():
    from web3 import Web3
    
    web_provider = Web3.HTTPProvider(Variable.get('infura_endpoint'))
    web3_instance = Web3(web_provider)

    start_block = int(Variable.get('block_rewards_start_block'))
    end_block = int(Variable.get('block_rewards_end_block'))

    print()
    print('start block: {}'.format(start_block))
    print('end block: {}'.format(end_block))
    print()

    new_start_block = min(end_block + 1, web3_instance.eth.block_number)
    new_end_block = min(new_start_block + 2000, web3_instance.eth.block_number)

    Variable.set(key = 'block_rewards_start_block', value = new_start_block)
    Variable.set(key = 'block_rewards_end_block', value = new_end_block)

    print()
    print('new start block: {}'.format(new_start_block))
    print('new end block: {}'.format(new_end_block))
    print()

schedule_interval = timedelta(minutes = 15)
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
        task_id = 'get_eth_block_rewards',
        retries = 3,
        retry_delay = timedelta(minutes = 1)
    )

    block_rewards_table_cols = ['block_no', 'miner_address', 'block_reward']
    s3_block_rewards_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_block_rewards_to_redshift',
        schema = 'eth_data',
        table = 'stg_block_reward',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/block_rewards_data/block_rewards.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = block_rewards_table_cols,
        copy_options = ["json 'auto'"],
        method = 'UPSERT',
        upsert_keys = ['block_no', 'miner_address'],
        retries = 3,
        retry_delay = timedelta(minutes = 1)
    )

    finish = PythonOperator(
        task_id = 'finish',
        python_callable = update_start_and_end_block,
        op_args = [],
        retries = 3,
        retry_delay = timedelta(minutes = 1)
    )

    block_rewards_to_s3 >> s3_block_rewards_to_redshift >> finish