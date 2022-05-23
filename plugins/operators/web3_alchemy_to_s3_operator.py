from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import BaseOperator
from airflow.models import Variable
from time import sleep
from pendulum import datetime
import datetime

import requests as r
import json

class Web3AlchemyToS3Operator(BaseOperator):

    def __init__(self, batch_size, node_endpoint,
                 bucket_name, is_historical_run, start_block_variable_name,
                 end_block_variable_name, **kwargs):
        super().__init__(**kwargs)

        self.batch_size = batch_size
        self.is_historical_run = is_historical_run

        self.node_endpoint = node_endpoint
        self.web3_instance = None

        self.bucket_name = bucket_name
        self.s3_connection = None

        self.start_block_variable_name = start_block_variable_name
        self.end_block_variable_name = end_block_variable_name

    ############################ HELPER FUNCTIONS ##########################################
    def __get_start_and_end_block(self):
        start_block = int(Variable.get(self.start_block_variable_name))
        end_block = int(Variable.get(self.end_block_variable_name))

        return start_block, end_block

    def __set_up_connections(self):
        from web3 import Web3
        
        web_provider = Web3.HTTPProvider(self.node_endpoint)
        self.web3_instance = Web3(web_provider)

        self.s3_connection = S3Hook(
            aws_conn_id = 's3_conn'
        )

    def __get_transfer_data(self):
        start_block, end_block = self.__get_start_and_end_block()
        ############################################ INTERNAL FUNC ############################################
        def get_transfer_data(page_key = None):
            headers = {'Content-Type':'application/json'}

            if page_key == None:
                params = {
                    'jsonrpc':'2.0',
                    'id':0,
                    'method':'alchemy_getAssetTransfers',
                    'params': [{
                        'fromBlock':'0x' + hex(start_block)[2:].upper(),
                        'toBlock':'0x' + hex(end_block)[2:].upper(),
                        'category': [
                            'erc20', 'external', 'internal'
                        ]
                    }]
                }
            else:
                params = {
                    'jsonrpc':'2.0',
                    'id':0,
                    'method':'alchemy_getAssetTransfers',
                    'params': [{
                        'fromBlock':'0x' + hex(start_block)[2:].upper(),
                        'toBlock':'0x' + hex(end_block)[2:].upper(),
                        'category': [
                            'erc20', 'external', 'internal'
                        ],
                        'pageKey':page_key
                    }]
                }

            response = r.post(
                url = Variable.get('alchemy_api_endpoint'),
                headers = headers,
                data = json.dumps(params)
            ).json()

            return response
        ############################################ INTERNAL FUNC END ########################################
        pagination_key = None
    
        preprocessed_transfers = []
        processed_transfers = []

        while True:
            transfer_response = get_transfer_data(pagination_key)
            error = transfer_response.get('error')

            if error != None and error.get('code') == -32604:
                print('one of the api calls to get transfer data timed out... sleeping for 5 minutes and trying again.')
                sleep(60 * 5)
                continue
            elif error != None and error.get('code') == 429:
                print('My app usage has exceeded its compute units per second capacity... Retrying request after 1 minute.')
                sleep(60)
                continue
            elif error != None:
                print('Error occurred... Sleeping for 1 minute and trying again.')
                sleep(60)
                continue

            result = transfer_response.get('result')

            if result == None:
                print('No data returned for this request... Sleeping for 5 minutes and trying again.')
                sleep(60 * 5)
                continue

            preprocessed_transfers.extend(result.get('transfers'))

            if result.get('pageKey') == None:
                break
            else:
                pagination_key = transfer_response.get('result').get('pageKey')
                continue

        for preprocessed_transfer in preprocessed_transfers:
            processed_transfers.append({
                'transaction_hash':preprocessed_transfer['hash'].lower(),

                'block_no':int(preprocessed_transfer['blockNum'], 0),

                'from_':preprocessed_transfer['from'].lower() if preprocessed_transfer['from'] != None else None,

                'to_':preprocessed_transfer['to'].lower() if preprocessed_transfer['to'] != None else None,

                'token_units':str(preprocessed_transfer['value']),

                'raw_token_units':str(preprocessed_transfer['rawContract']['value']),

                'asset':preprocessed_transfer['asset'] if ((preprocessed_transfer['asset'] != None) and (len(preprocessed_transfer['asset']) <= 16)) else None,

                'transfer_category':preprocessed_transfer['category'],

                'token_address': preprocessed_transfer['rawContract']['address'].lower() if preprocessed_transfer['rawContract']['address'] != None else None
            })

        return processed_transfers

    def __get_block_data(self):
        from web3.exceptions import BlockNotFound

        start_block, end_block = self.__get_start_and_end_block()

        print()
        print('fetching data between blocks {} and {} ({} total)'.format(start_block, end_block, end_block - start_block + 1))
        print()
        
        transaction_batch = []
        block_batch = []
        preprocessed_blocks = []

        while True:
            try:
                preprocessed_blocks = [self.web3_instance.eth.get_block(i, full_transactions = True) for i in range(start_block, end_block + 1)]
                break
            except BlockNotFound:
                print()
                print('One of the blocks could not be found... Sleeping for 5 minutes and trying again.')
                print()
                sleep(60 * 5)
                continue
                
        for block in preprocessed_blocks:            
            block_batch.append({
                'block_no':int(block['number']),

                'block_hash':block['hash'].hex().lower(),

                'parent_hash':block['parentHash'].hex().lower(),

                'timestamp':int(block['timestamp']),

                'date': datetime.datetime.fromtimestamp(int(block['timestamp'])).date().strftime('%m/%d/%Y'),

                'difficulty':int(block['difficulty']),

                'miner_address':block['miner'].lower(),

                'gas_used':int(block['gasUsed']),

                'block_size':int(block['size']),

                'sha3_uncles':block['sha3Uncles'].hex().lower(),
                
                'gas_limit':int(block['gasLimit'])
            })
            transaction_batch.append(block['transactions'])

        return block_batch, transaction_batch

    def __get_transaction_data(self, transaction_batch):
        preprocessed_transactions = []
        processed_transactions = []

        start_block, end_block = self.__get_start_and_end_block()

        for transactions in transaction_batch:
            preprocessed_transactions.extend(transactions)

        for transaction in preprocessed_transactions:
            processed_transactions.append({
                'transaction_hash':transaction['hash'].hex().lower(),
                
                'block_no':int(transaction['blockNumber']),

                'to_':transaction['to'].lower() if transaction['to'] != None else None,

                'from_':transaction['from'].lower() if transaction['from'] != None else None,

                'value':int(transaction['value']) / (10 ** 18),

                'gas':int(transaction['gas']),
                
                'gas_price':int(transaction['gasPrice'])
            })
            
        return processed_transactions

    def __upload_to_s3(self, block_data, transaction_data, transfer_data):
        if self.is_historical_run:
            dest_folder = 'eth_data_historical/' + self.start_block_variable_name + '-' + self.end_block_variable_name
        else:
            dest_folder = 'eth_data'
            
        json_block_data = json.dumps(block_data).replace('[', '').replace(']', '').replace('},', '}')
        self.s3_connection.load_string(
            json_block_data,
            key = '{}/blocks.json'.format(dest_folder),
            bucket_name = self.bucket_name,
            replace = True
        )

        json_transaction_data = json.dumps(transaction_data).replace('[', '').replace(']', '').replace('},', '}')
        self.s3_connection.load_string(
            json_transaction_data,
            key = '{}/transactions.json'.format(dest_folder),
            bucket_name = self.bucket_name,
            replace = True
        )

        json_transfer_data = json.dumps(transfer_data).replace('[', '').replace(']', '').replace('},', '}')
        self.s3_connection.load_string(
            json_transfer_data,
            key = '{}/transfers.json'.format(dest_folder),
            bucket_name = self.bucket_name,
            replace = True
        )

    ############################ HELPER FUNCTIONS END ######################################

    def execute(self, context):
        start_block, end_block = self.__get_start_and_end_block()

        if start_block == end_block:
            raise Exception('Got data for every block in assigned interval')

        self.__set_up_connections()
        
        print()
        print('start block: {}'.format(start_block))
        print('end block: {}'.format(end_block))
        print()
       
        block_data, transaction_batch = self.__get_block_data()
        transaction_data = self.__get_transaction_data(transaction_batch)
        transfer_data = self.__get_transfer_data()
        
        self.__upload_to_s3(block_data, transaction_data, transfer_data)