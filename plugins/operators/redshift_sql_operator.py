from airflow.models import BaseOperator
from airflow.models import Variable

class RedshiftSQLOperator(BaseOperator):
    def __init__(self, queries, **kwargs):
        super().__init__(**kwargs)
        self.queries = queries
        
    def execute(self, context):
        with redshift_connector.connect(
            host = Variable.get('redshift_host'),
            user = 'administrator',
            password = Variable.get('redshift_password')
        ) as conn:
            with conn.cursor() as cursor:
                for query in self.queries:
                    cursor.execute(query)
                
                conn.commit()
