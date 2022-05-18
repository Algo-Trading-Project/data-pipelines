from operators.redshift_sql_operator import RedshiftSQLOperator
from datetime import timedelta
import pendulum

from airflow import DAG

schedule_interval = timedelta(days = 1)
start_date = pendulum.datetime(year = 2022,
                               month = 5,
                               day = 17,
                               hour = 7,
                               tz = 'America/Los_Angeles')

with DAG(
    dag_id = 'stg_transaction_to_prod',
    schedule_interval = schedule_interval,
    start_date = start_date,
    catchup = False,
    max_active_runs = 1
) as dag:
    move_staging_data_to_prod = RedshiftSQLOperator(
        task_id = 'move_staging_data_to_prod',
        queries = [
            """
            BEGIN;
            """,

            """
            INSERT INTO administrator.eth_data.transaction (
                SELECT *
                FROM 
                    administrator.eth_data.stg_transaction
                WHERE 
                    block_no >= (
                        SELECT MAX(block_no)
                        FROM administrator.eth_data.transaction
                    )
            );
            """,

            """
            DELETE FROM administrator.eth_data.stg_transaction
            WHERE 
                block_no <= (
                    SELECT MAX(block_no)
                    FROM administrator.eth_data.transaction
                );
            """,

            """
            COMMIT;
            """
        ]
    )

    move_staging_data_to_prod 

