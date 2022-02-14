from airflow.plugins_manager import AirflowPlugin
from operators.web3_alchemy_to_s3_operator import Web3AlchemyToS3Operator

class web3_alchemy_to_s3_plugin(AirflowPlugin):

    name = 'web3_alchemy_to_s3_plugin'       
    operators = [Web3AlchemyToS3Operator]
