from airflow.models import BaseOperator
from airflow.models import Variable

class RedshiftSQLOperator(BaseOperator):
    """
    An Airflow operator to execute SQL queries in an Amazon Redshift database.

    This operator allows for the execution of a list of SQL queries on an Amazon Redshift
    cluster. It uses Airflow's Variable mechanism to securely access the Redshift cluster
    credentials. The operator establishes a connection to the Redshift cluster, executes 
    all provided SQL queries, and commits the transactions.

    Attributes:
        queries (list of str): A list of SQL query strings to be executed on the Redshift cluster.
    """

    def __init__(self, queries, **kwargs):
        """
        Initialize the RedshiftSQLOperator with a set of queries to be executed.

        Parameters:
            queries (list of str): A list of SQL query strings that the operator will execute.
                                   Each query is executed sequentially.
            **kwargs: Additional keyword arguments to pass to the BaseOperator.
        """
        super().__init__(**kwargs)
        self.queries = queries
        
    def execute(self, context):
        """
        Execute the SQL queries on the Redshift cluster.

        This method is called by Airflow to run the task. It establishes a connection to the
        Redshift cluster using the credentials stored in Airflow's Variables, executes each 
        SQL query in the provided list, and commits the transactions.

        Parameters:
            context (dict): The Airflow context for the task execution, providing information
                            such as the execution date and task instance.

        Returns:
            None
        """
        with redshift_connector.connect(
            host = Variable.get('redshift_host'),
            user = 'administrator',
            password = Variable.get('redshift_password')
        ) as conn:
            with conn.cursor() as cursor:
                for query in self.queries:
                    cursor.execute(query)
                
                conn.commit()
