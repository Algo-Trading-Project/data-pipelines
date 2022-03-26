from airflow import DAG
from airflow.models import Variable
from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import State
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum

schedule_interval = timedelta(minutes = 15)
start_date = pendulum.datetime(year = 2022,
                               month = 3,
                               day = 26,
                               hour = 11,
                               minute = 30,
                               tz = 'America/Los_Angeles')

def update_start_and_end_block(i, end_block):
    new_start_block = None
    new_end_block = None

    start_block = int(Variable.get('start_block_{}'.format(i)))
   
    print()
    print('start block: {}'.format(start_block))
    print('end block: {}'.format(end_block))
    print()

    if i == 1:
        new_start_block = min(end_block + 1, 3750000)
        new_end_block = min(new_start_block + 2000, 3750000)

    elif i == 2:
        new_start_block = min(end_block + 1, 7500000)
        new_end_block = min(new_start_block + 2000, 7500000)

    elif i == 3:
        new_start_block = min(end_block + 1, 11250000)
        new_end_block = min(new_start_block + 2000, 11250000)

    elif i == 4:
        new_start_block = min(end_block + 1, 14450000)
        new_end_block = min(new_start_block + 2000, 14450000)

    Variable.set(key = 'start_block_{}'.format(i), value = new_start_block)
    Variable.set(key = 'end_block_{}'.format(i), value = new_end_block)

    print()
    print('new start block: {}'.format(new_start_block))
    print('new end block: {}'.format(new_end_block))
    print()

for i in range(1, 5):
    dag_id = 'fetch_historical_eth_data_{}'.format(i)
    dag = DAG(dag_id,
              start_date = start_date, 
              schedule_interval = schedule_interval)

    with dag:
        previous_dag_run_sensor = ExternalTaskSensor(
            task_id = 'check_previous_dag_run_finished',
            external_dag_id = dag_id,
            external_task_id = 'dag_run_finished',
            allowed_states = [State.SUCCESS, State.NONE],
            failed_states = [State.FAILED, State.RUNNING, State.QUEUED],
            execution_delta = schedule_interval
        )

        eth_data_to_s3 = Web3AlchemyToS3Operator(
            task_id = 'get_historical_eth_data',
            batch_size = 2000,
            node_endpoint = Variable.get('infura_endpoint'),
            bucket_name = 'project-poseidon-data',
            is_historical_run = True,
            start_block_variable_name = 'start_block_{}'.format(i),
            end_block_variable_name = 'end_block_{}'.format(i)
        )     

        s3_key_prefix = 'eth_data_historical/start_block_{}-end_block_{}'.format(i, i)

        block_table_cols = ['block_no', 'block_hash', 'parent_hash', 'timestamp',
                            'difficulty', 'miner_address', 'gas_used',
                            'block_size', 'sha3_uncles', 'gas_limit']
        s3_block_data_to_redshift = S3ToRedshiftOperator(
            task_id = 's3_eth_block_data_to_redshift',
            schema = 'eth_data',
            table = 'block',
            method = 'UPSERT',
            upsert_keys = ['block_no'],
            s3_bucket = 'project-poseidon-data',
            s3_key = '{}/blocks.json'.format(s3_key_prefix),
            redshift_conn_id = 'redshift_conn',
            aws_conn_id = 's3_conn',
            column_list = block_table_cols,
            copy_options = ["json 'auto'"]
        )

        transaction_table_cols = ['transaction_hash', 'block_no', 'to_',
                                    'from_', 'value', 'gas', 'gas_price']
        s3_transaction_data_to_redshift = S3ToRedshiftOperator(
            task_id = 's3_eth_transaction_data_to_redshift',
            schema = 'eth_data',
            table = 'transaction',
            method = 'UPSERT',
            upsert_keys = ['transaction_hash'],
            s3_bucket = 'project-poseidon-data',
            s3_key = '{}/transactions.json'.format(s3_key_prefix),
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
            table = 'transfer',
            s3_bucket = 'project-poseidon-data',
            s3_key = '{}/transfers.json'.format(s3_key_prefix),
            redshift_conn_id = 'redshift_conn',
            aws_conn_id = 's3_conn',
            column_list = transfer_event_table_cols,
            copy_options = ["json 'auto'"]
        )

        finish = PythonOperator(
            task_id = 'dag_run_finished',
            python_callable = update_start_and_end_block,
            op_args = [i, int(Variable.get('end_block_{}'.format(i)))]
        )

        previous_dag_run_sensor >> eth_data_to_s3 >> [s3_block_data_to_redshift, s3_transaction_data_to_redshift, s3_transfer_data_to_redshift] >> finish
    
    globals()[dag_id] = dag
    