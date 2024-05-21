from airflow.models import BaseOperator
from airflow.models import Variable
from time import sleep
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as r
import json

class GetEthTransactionReceiptsOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.s3_connection = S3Hook(aws_conn_id='s3_conn')

        infura_endpoint = 'https://mainnet.infura.io/v3/{}'.format(Variable.get('infura_api_key'))
        eth_node = Web3(Web3.HTTPProvider(infura_endpoint))
        self.web3_instance = eth_node

        self.start_block = int(Variable.get('transaction_gas_used_start_block'))
        self.end_block = int(Variable.get('transaction_gas_used_end_block'))

        self.counter = 0

    def __update_start_and_end_block_airflow(self, start_block, end_block):
        Variable.set('transaction_gas_used_start_block', start_block)
        Variable.set('transaction_gas_used_end_block', end_block)

    def __upload_to_s3(self, transaction_receipt_data):
        if len(transaction_receipt_data) == 0:
            return

        key = 'eth_data/transaction_receipts_data/transaction_receipts.json.{}'.format(self.counter)
        data_to_upload = json.dumps(transaction_receipt_data).replace('[', '').replace(']', '').replace('},', '}')

        self.s3_connection.load_string(
            data_to_upload,
            bucket_name = 'project-poseidon-data',
            key = key,
            replace = True
        )

        self.counter += 1

    def __get_block_transaction_receipts(self, block_num, max_retries = 1):
        headers = {
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        params = {
            'jsonrpc': '2.0',
            'method': 'alchemy_getTransactionReceipts',
            'params':[
                {
                    'blockNumber': '0x' + hex(block_num)[2:].upper()
                }
            ],
            'id': 1
        }

        num_retries = 0

        while True:
            if num_retries > max_retries:
                self.log.error('GetEthTransactionGasUsed: Max retries exceeded for block {}'.format(block_num))
                self.log.error('GetEthTransactionGasUsed:')

                return None

            try:
                request_url = 'https://eth-mainnet.g.alchemy.com/v2/{}'.format(Variable.get('alchemy_api_key'))
                response = r.post(
                    url = request_url,
                    headers = headers,
                    json = params
                )
            except Exception as e:
                self.log.error('GetEthTransactionGasUsed: Error getting transaction receipts for block {}'.format(block_num))
                self.log.error('GetEthTransactionGasUsed: {}'.format(str(e)))
                self.log.error('GetEthTransactionGasUsed:')

                return None

            response = response.json()
            error = response.get('error')
            request_result = response.get('result')

            if error is not None:
                self.log.error('GetEthTransactionGasUsed: Error - {}'.format(response.json()))
                self.log.error('GetEthTransactionGasUsed:')

                return None
            else:
                return request_result.get('receipts')

     ####### HELPER FUNCTIONS END ########

    def __get_block_transaction_receipts_concurrently(self, block_numbers, max_retries = 1):
        results = []
        failed_requests = set()

        with ThreadPoolExecutor(max_workers = 10) as executor:
            futures = [executor.submit(self.__get_block_transaction_receipts, block_num) for block_num in block_numbers]
            future_to_block = {future: block_num for future, block_num in zip(futures, block_numbers)}

            for future in as_completed(futures):
                block_num = future_to_block[future]

                try:
                    result = future.result()
                    if result is None:
                        failed_requests.add(block_num)
                    else:
                        results.extend(result)
                except Exception as e:
                    failed_requests.add(block_num)

        # Retry logic for failed requests
        successfully_retried_requests = set()

        for block_num in failed_requests:
            for _ in range(max_retries):
                try:
                    result = self.__get_block_transaction_receipts(block_num)
                    if result is None:
                        continue
                    else:
                        results.extend(result)
                        successfully_retried_requests.add(block_num)
                        break
                except Exception as e:
                    continue

        failed_requests -= successfully_retried_requests

        for block_num in failed_requests:
            self.log.error(f'GetEthTransactionReceiptsOperator: Request for block {block_num} failed after {max_retries} retries.')

        return results

    def execute(self, context):
        start_block, end_block = self.start_block, self.end_block
        processed_transaction_receipts = []

        while True:

            if end_block <= start_block:
                self.log.info('GetEthTransactionGasUsed: Reached the beginning of the chain at block {}'.format(start_block))
                self.__upload_to_s3(processed_transaction_receipts)
                return

            if len(processed_transaction_receipts) >= 10000:
                self.log.info('GetEthTransactionGasUsed: Uploading data to S3')
                self.__upload_to_s3(processed_transaction_receipts)
                self.__update_start_and_end_block_airflow(start_block, end_block)
                processed_transaction_receipts = []

            self.log.info(f'GetEthTransactionGasUsed: Processing blocks {end_block} to {start_block}')
            self.log.info('GetEthTransactionGasUsed:')

            block_range = list(range(end_block, start_block - 1, -1))
            transaction_receipts =  self.__get_block_transaction_receipts_concurrently(block_range)

            if len(transaction_receipts) == 0:
                self.log.error(f'GetEthTransactionGasUsed: No transaction receipts returned for blocks {end_block} to {start_block}')
                self.log.error('GetEthTransactionGasUsed:')

                self.__upload_to_s3(processed_transaction_receipts)

                return

            for receipt in transaction_receipts:
                
                transaction_hash = receipt['transactionHash'].lower() if receipt['transactionHash'] is not None else None
                block_no = int(receipt['blockNumber'], 16) if receipt['blockNumber'] is not None else None
                from_ = receipt['from'].lower() if receipt['from'] is not None else None
                to_ = receipt['to'].lower() if receipt['to'] is not None else None
                contract_address = receipt['contractAddress'].lower() if receipt['contractAddress'] is not None else None
                status = int(receipt['status'], 16) if receipt['status'] is not None else None
                effective_gas_price = float(int(receipt['effectiveGasPrice'], 16)) / (10 ** 18) if receipt['effectiveGasPrice'] is not None else None
                gas_used = int(receipt['gasUsed'], 16) if receipt['gasUsed'] is not None else None
                type_ = receipt['type'] if receipt['type'] is not None else None

                processed_transaction_receipts.append({
                    'transaction_hash': transaction_hash,
                    'block_no': block_no,
                    'from': from_,
                    'to': to_,
                    'contract_address': contract_address,
                    'status': status,
                    'effective_gas_price': effective_gas_price,
                    'gas_used': gas_used,
                    'type': type_
                })

            end_block = max(start_block - 1, 0)
            start_block = max(end_block - 1000, 0)