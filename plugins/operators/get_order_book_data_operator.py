from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from datetime import timedelta

import requests as r
import json
import pandas as pd
import dateutil.parser as parser

class GetOrderBookDataOperator(BaseOperator):
    """
    Operator that gets CoinAPI order book snapshots for tokens in DESIRED_TOKENS and stores them in S3.
    """
    
    # List of tokens to get order book data for
    DESIRED_TOKENS = [
        'BTC_USD_COINBASE', 'ETH_USD_COINBASE', 'BCH_USD_COINBASE',
        'ETC_USD_COINBASE', 'BNB_USDC_BINANCE', 'LINK_USD_COINBASE',
        'XRP_USDT_BINANCE', 'MATIC_USDT_BINANCE', 'EOS_USD_COINBASE', 
        'ZRX_USD_COINBASE', 'LTC_USD_COINBASE', 'ATOM_USDT_BINANCE'
    ]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
        # S3 connection
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

        # File chunk number for order book data
        self.s3_file_chunk_num = 1

        # List of order book snapshots for current token
        self.order_book_snapshots = []

        # Token metadata stored in S3
        self.token_metadata_df = self._get_coinapi_metadata()

    @staticmethod
    def on_task_failure(context):
        """
        Callback function that gets called if an instance of this task fails.

        Parameters:
            context - Airflow context object

        Returns:
            None
        """
        
        # Access the operator instance via context
        operator_instance = context['task_instance'].task
        
        # Upload any data collected before the task failed to S3
        operator_instance._upload_new_order_book_data(operator_instance.order_book_snapshots)

        # Update token metadata stored in S3 with new scrape dates
        operator_instance._upload_updated_coinapi_metadata(operator_instance.token_metadata_df)

    # Gets the next date to scrape order book data for current token
    def _get_next_start_date(self, coinapi_token):
        def hour_rounder(t):
            # Round pandas datetime object t up to the nearest hour
            rounded_hour = t.replace(second = 0, microsecond = 0, minute = 0, hour = t.hour) + timedelta(hours = 1)
            return rounded_hour.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get the next date to scrape order book data for current token
        next_start_date = coinapi_token['latest_scrape_date_orderbook']

        # If we have never scraped order book data for this token before
        if pd.isnull(next_start_date):
            # Get the date of the first order book snapshot for this token
            next_start_date = pd.to_datetime(coinapi_token['data_orderbook_start'])
            # Round the date up to the nearest hour
            next_start_date = hour_rounder(next_start_date)
            # Convert the date to ISO 8601 format
            next_start_date = parser.parse(str(next_start_date)).isoformat().split('+')[0]
        
        # If we have scraped order book data for this token before
        else:
            # Convert the date to ISO 8601 format
            next_start_date = parser.parse(str(next_start_date)).isoformat().split('+')[0]

        return next_start_date

    # Uploads new order book data to S3
    def _upload_new_order_book_data(self, new_order_book_data):
        
        # If there is no new order book data to upload then return
        if len(new_order_book_data) == 0:
            return
        
        # Else upload new order book data to S3
        data_to_uplaod = json.dumps(new_order_book_data).replace('[', '').replace(']', '').replace('},', '}')
        key = 'eth_data/order_book_data/coinapi_pair_order_book_snapshot_1_hour.json.{}'.format(self.s3_file_chunk_num)

        self.s3_connection.load_string(
            string_data = data_to_uplaod, 
            key = key, 
            bucket_name = 'project-poseidon-data', 
            replace = True
        )

        # Increment file chunk number
        self.s3_file_chunk_num += 1

    # Uploads updated token metadata to S3
    def _upload_updated_coinapi_metadata(self, token_metadata_df):
        
        # Convert updated token metadata DF to JSON
        token_metadata_df_json = token_metadata_df.to_dict(orient = 'records')
        token_metadata_str = json.dumps(token_metadata_df_json)

        # Upload updated token metadata to S3
        self.s3_connection.load_string(
            string_data = token_metadata_str,
            key = 'eth_data/metadata/coinapi_pair_metadata.json',
            bucket_name = 'project-poseidon-data',
            replace = True
        )

    # Updates next scrape date for current token locally
    def _update_coinapi_metadata(self, time_start, token_metadata_df, coinapi_token):
        asset_id_base = coinapi_token['asset_id_base']
        asset_id_quote = coinapi_token['asset_id_quote']
        exchange_id = coinapi_token['exchange_id']

        # Next scrape date is one hour after the current scrape date
        next_scrape_date = str(pd.to_datetime(time_start) + pd.Timedelta(hours = 1))
        next_scrape_date = parser.parse(next_scrape_date).isoformat()

        # Update next scrape date for current token locally
        predicate = (token_metadata_df['exchange_id'] == exchange_id) & (token_metadata_df['asset_id_base'] == asset_id_base) & (token_metadata_df['asset_id_quote'] == asset_id_quote)
        token_metadata_df.loc[predicate, 'latest_scrape_date_orderbook'] = next_scrape_date

        # Return updated token metadata DF
        return token_metadata_df
    
    # Deletes order book data from S3
    def _delete_order_book_data_from_s3(self):

        # Get keys for order book data stored in S3
        keys_to_delete = self.s3_connection.list_keys(
            bucket_name = 'project-poseidon-data',
            prefix = 'eth_data/order_book_data'
        )

        # Delete order book data from S3
        self.s3_connection.delete_objects(
            bucket = 'project-poseidon-data', 
            keys = keys_to_delete
        )
        
    # Gets order book snapshot from CoinAPI for a given token and date
    def _get_order_book_snapshot(self, coinapi_symbol_id, time_start):
        
        # Formats response data from CoinAPI
        def format_response_data(response):
            exchange_id, symbol_id, asset_id_base, asset_id_quote = coinapi_symbol_id.split('_')
            
            for elem in response:
                elem['bids'] = json.dumps(elem['bids'])
                elem['asks'] = json.dumps(elem['asks'])
                
                elem['exchange_id'] = exchange_id
                elem['asset_id_base'] = asset_id_base
                elem['asset_id_quote'] = asset_id_quote

            return response

        # Make request to CoinAPI
        api_request_url = 'https://rest.coinapi.io/v1/orderbooks/{}/history?time_start={}&limit={}&apikey={}'.format(coinapi_symbol_id, time_start, 1, Variable.get('COINAPI_API_KEY'))

        print('API Request URL: {}'.format(api_request_url))
        print()
        print()
        
        try:
            response = r.get(url = api_request_url)
            print(response.json())
            print()
        except:
            print('An error occurred while making the request')
            print()
            return -1

        # Request successful
        if response.status_code == 200:
            print('Request Successful')
            print()

            response_json = response.json()
            formatted_response = format_response_data(response_json)
            return formatted_response
        
        # Bad Request -- There is something wrong with your request
        elif response.status_code == 400:
            print('Bad Request -- There is something wrong with your request')
            print()
            return response.status_code
        
        # Unauthorized -- Your API key is wrong
        elif response.status_code == 401:
            print('Unauthorized -- Your API key is wrong')
            print()
            return response.status_code
        
        # Forbidden -- Your API key doesnt't have enough privileges to access this resource
        elif response.status_code == 403:
            print("Forbidden -- Your API key doesn't have enough privileges to access this resource")
            print()
            return response.status_code
        
        # Too many requests -- You have exceeded your API key rate limits
        elif response.status_code == 429:
            print('Too many requests -- You have exceeded your API key rate limits')
            print()
            return response.status_code

        # No data -- You requested specific single item that we don't have at this moment.
        elif response.status_code == 550:
            print("No data -- You requested specific single item that we don't have at this moment.")
            print()
            return response.status_code
        
        # Unknown error
        else:
            print('Unknown error')
            return -1
   
    # Gets token metadata stored in S3
    def _get_coinapi_metadata(self):
        
        # S3 key for token metadata (next scrape dates)
        key = 'eth_data/metadata/coinapi_pair_metadata.json'

        # Read token metadata from S3 and load it into a DataFrame
        token_metadata_str = self.s3_connection.read_key(key = key, bucket_name = 'project-poseidon-data')
        token_metadata_json = json.loads(token_metadata_str)
        token_metadata_df = pd.DataFrame(token_metadata_json)

        # Filter out tokens we don't want to get order book data for
        base_quote_exchange = token_metadata_df['asset_id_base'] + '_' + token_metadata_df['asset_id_quote'] + '_' + token_metadata_df['exchange_id']
        predicate = base_quote_exchange.isin(self.DESIRED_TOKENS)
        token_metadata_df = token_metadata_df[predicate]

        return token_metadata_df

    def execute(self, context):
        """
        Gets CoinAPI order book snapshots for tokens in DESIRED_TOKENS and stores them in S3.

        Parameters:
            context - Airflow context object

        Returns:
            None
        """

        # Delete order book data potentially stored in S3 from previous task runs
        self._delete_order_book_data_from_s3()
    
        # For each token in DESIRED_TOKENS
        for i in range(len(self.token_metadata_df)):

            while True:

                # Every time we have collected 24 * 30 order book snapshots (~1 month's worth of data)
                if len(self.order_book_snapshots) % 24 * 30 == 0:
                    
                    print('One month of data collected... uploading to S3 and updating metadata.')
                    print()

                    # Upload new order book snapshots to S3
                    self._upload_new_order_book_data(self.order_book_snapshots)

                    # Update token metadata stored in S3
                    self._upload_updated_coinapi_metadata(self.token_metadata_df)

                    # Empty list of order book snapshots for current token
                    self.order_book_snapshots = []

                # Get token metadata for current token
                coinapi_token = self.token_metadata_df.iloc[i]

                print('{}) token: {}/{} (exchange: {})'.format(i + 1, coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']))
                print()
                
                # Get CoinAPI symbol ID for current token
                coinapi_symbol_id = coinapi_token['exchange_id'] + '_' + 'SPOT' + '_' + coinapi_token['asset_id_base'] + '_' + coinapi_token['asset_id_quote']

                # Get the next date to scrape order book data for current token
                time_start = self._get_next_start_date(coinapi_token)

                # If time_start is a date in the future
                if pd.to_datetime(time_start) > pd.to_datetime('now'):
                    # Stop scraping order book data for current token
                    print('time_start is a date in the future... continuing to next token.')
                    print()
                    break
            
                # Get order book snapshot on time_start for current token
                order_book_snapshot = self._get_order_book_snapshot(coinapi_symbol_id = coinapi_symbol_id, time_start = time_start)
                
                # If request didn't succeed
                if type(order_book_snapshot) == int:

                    # If we have exceeded our API key rate limits then update token metadata stored in
                    # S3, upload new data collected thus far to S3, and stop this task
                    if order_book_snapshot == 429:
                        self._upload_new_order_book_data(self.order_book_snapshots)
                        self._upload_updated_coinapi_metadata(self.token_metadata_df)
                        return

                    # Else just proceed to the next token
                    else:
                        break

                # If request succeeded
                else:

                    # If request returned an empty response
                    if len(order_book_snapshot) == 0:
                        print('No data returned for request... continuing to next token.')
                        print()
                        break
                    
                    # If request returned a non-empty response
                    else:
                        print('Data returned for ({})... continuing to next request.'.format(time_start))
                        
                        # Add to list of order book snapshots for current token
                        self.order_book_snapshots.extend(order_book_snapshot)

                        # Update metadata for this token locally
                        self.token_metadata_df = self._update_coinapi_metadata(
                            time_start,
                            self.token_metadata_df,
                            coinapi_token
                        )

            # Upload new order book data collected for current token to S3
            self._upload_new_order_book_data(self.order_book_snapshots)

            # Update token metadata stored in S3
            self._upload_updated_coinapi_metadata(self.token_metadata_df)