from airflow import DAG
from airflow.models import Variable
from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum

def update_start_and_end_block(end_block):
    from web3 import Web3

    eth_node = Web3(Web3.HTTPProvider(Variable.get('infura_endpoint')))
    start_block = int(Variable.get('start_block'))
   
    print()
    print('start block: {}'.format(start_block))
    print('end block: {}'.format(end_block))
    print()

    new_start_block = min(end_block + 1, eth_node.eth.block_number)
    new_end_block = min(new_start_block + 1000, eth_node.eth.block_number)

    Variable.set(key = 'start_block', value = new_start_block)
    Variable.set(key = 'end_block', value = new_end_block)

    print()
    print('new start block: {}'.format(new_start_block))
    print('new end block: {}'.format(new_end_block))
    print()

schedule_interval = timedelta(minutes = 15)
start_date = pendulum.datetime(year = 2022,
                               month = 3,
                               day = 26,
                               hour = 5,
                               tz = 'America/Los_Angeles')

with DAG('fetch_new_eth_data',
          start_date = start_date, 
          schedule_interval = schedule_interval,
          catchup = False,
          max_active_runs = 1
          ) as dag:

    eth_data_to_s3 = Web3AlchemyToS3Operator(
        task_id = 'get_eth_data',
        batch_size = 1000,
        node_endpoint = Variable.get('infura_endpoint'),
        bucket_name = 'project-poseidon-data',
        is_historical_run = False,
        start_block_variable_name = 'start_block',
        end_block_variable_name = 'end_block'
    )     

    block_table_cols = ['block_no', 'block_hash', 'parent_hash', 'timestamp', 'date',
                        'difficulty', 'miner_address', 'gas_used',
                        'block_size', 'sha3_uncles', 'gas_limit']
    s3_block_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_block_data_to_redshift',
        schema = 'eth_data',
        table = 'stg_block',
        method = 'UPSERT',
        upsert_keys = ['block_no'],
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/blocks.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = block_table_cols,
        copy_options = ["json 'auto'",  "DATEFORMAT AS 'MM/DD/YYYY'"]
    )

    transaction_table_cols = ['transaction_hash', 'block_no', 'to_',
                              'from_', 'value', 'gas', 'gas_price']
    s3_transaction_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_transaction_data_to_redshift',
        schema = 'eth_data',
        table = 'stg_transaction',
        method = 'UPSERT',
        upsert_keys = ['transaction_hash'],
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transactions.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = transaction_table_cols,
        copy_options = ["json 'auto'"]
    )

    transfer_event_table_cols = ['transaction_hash', 'block_no', 'from_',
                                 'to_', 'token_units', 'raw_token_units',
                                 'asset', 'transfer_category', 'token_address']
    s3_transfer_data_to_redshift = S3ToRedshiftOperator(
        task_id = 's3_eth_transfer_data_to_redshift',
        schema = 'eth_data',
        table = 'stg_transfer',
        s3_bucket = 'project-poseidon-data',
        s3_key = 'eth_data/transfers.json',
        redshift_conn_id = 'redshift_conn',
        aws_conn_id = 's3_conn',
        column_list = transfer_event_table_cols,
        copy_options = ["json 'auto'"]
    )

    finish = PythonOperator(
        task_id = 'dag_run_finished',
        python_callable = update_start_and_end_block,
        op_args = [int(Variable.get('end_block'))]
    )

    eth_data_to_s3 >> [s3_block_data_to_redshift, s3_transaction_data_to_redshift, s3_transfer_data_to_redshift] >> finish