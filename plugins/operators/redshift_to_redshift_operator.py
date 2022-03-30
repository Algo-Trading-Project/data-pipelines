from airflow.models import BaseOperator
import redshift_connector
from airflow.models import Variable

class RedshiftToRedshiftOperator(BaseOperator):

    def __init__(self, database, schema, table, query):
        self.query = query
        
        self.database = database
        self.schema = schema
        self.table = table

    def execute(self, context):
        with redshift_connector.connect(
            host = Variable.get('redshift_host'),
            database = 'administrator',
            user = 'administrator',
            password = Variable.get('redshift_password')
        ) as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO {}.{}.{} VALUES (
                    ({})
                )
                """.format(self.database, self.schema, self.table, self.query)
                cursor.execute(query)
                conn.commit()
                        