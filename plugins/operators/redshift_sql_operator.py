from airflow.models import BaseOperator
import redshift_connector
from airflow.models import Variable

class RedshiftSQLOperator(BaseOperator):

    def __init__(self, query):
        self.query = query
        
    def execute(self, context):
        with redshift_connector.connect(
            host = Variable.get('redshift_host'),
            user = 'administrator',
            password = Variable.get('redshift_password')
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(self.query)
                conn.commit()
                        