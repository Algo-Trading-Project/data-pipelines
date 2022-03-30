from time import sleep
from airflow.models import BaseOperator

class CheckPreviousDagRunOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        curr_dag_run = context['dag_run']
        prev_dag_run = curr_dag_run.get_previous_dagrun()

        if prev_dag_run == None:
            print()
            print('No previous dag run... Starting first one.')
            print()
            return

        prev_dag_run_state = prev_dag_run.get_state()

        while True:
            if prev_dag_run_state == 'success':
                print()
                print('Previous dag run finished...  Starting next one.') 
                print()
                break
            elif prev_dag_run_state == 'failed':
                print()
                print('Previous dag run failed... Starting next one.')
                print()
                break
            elif prev_dag_run_state == 'running':
                print()
                print('Previous dag run still running... Will check again in 5 minutes.')
                print()

                sleep(60 * 5)
                prev_dag_run_state = prev_dag_run.get_state()
                continue