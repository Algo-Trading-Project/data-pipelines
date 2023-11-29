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
        'ADA_USDT_BINANCE', 'BNB_USDC_BINANCE', 'XRP_USDT_BINANCE'
    ]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
        # S3 connection
        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')

        # File chunk number for order book data
        self.s3_file_chunk_num = 1

    # Gets the next date to scrape order book data for current token
    def __get_next_start_date(self, coinapi_pair):
        def hour_rounder(t):
            # Round pandas datetime object t up to the nearest hour
            rounded_hour = t.replace(second = 0, microsecond = 0, minute = 0, hour = t.hour) + timedelta(hours = 1)
            return rounded_hour.strftime('%Y-%m-%d %H:%M:%S')
        
        # Get the next date to scrape order book data for current token
        next_start_date = coinapi_pair['latest_scrape_date_orderbook']

        # If we have never scraped order book data for this token before
        if pd.isnull(next_start_date):
            # Get the date of the first order book snapshot for this token
            next_start_date = pd.to_datetime(coinapi_pair['data_orderbook_start'])
            # Round the date up to the nearest hour
            next_start_date = hour_rounder(start_date)
            # Convert the date to ISO 8601 format
            next_start_date = parser.parse(str(next_start_date)).isoformat().split('+')[0]
        
        # If we have scraped order book data for this token before
        else:
            # Convert the date to ISO 8601 format
            next_start_date = parser.parse(str(most_recent_data_date)).isoformat().split('+')[0]

        return next_start_date

    # Uploads new order book data to S3
    def __upload_new_order_book_data(self, new_order_book_data):
        
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
    def __upload_updated_coinapi_metadata(self, coinapi_pairs_df):
        
        # Convert updated token metadata DF to JSON
        coinapi_pairs_df_json = coinapi_pairs_df.to_dict(orient = 'records')
        coinapi_pairs_str = json.dumps(coinapi_pairs_df_json)

        # Upload updated token metadata to S3
        self.s3_connection.load_string(
            string_data = coinapi_pairs_str,
            key = 'eth_data/metadata/coinapi_pair_metadata.json',
            bucket_name = 'project-poseidon-data',
            replace = True
        )

    # Updates next scrape date for current token locally
    def __update_coinapi_metadata(self, time_start, coinapi_pairs_df, coinapi_pair):
        asset_id_base = coinapi_pair['asset_id_base']
        asset_id_quote = coinapi_pair['asset_id_quote']
        exchange_id = coinapi_pair['exchange_id']

        # Next scrape date is one hour after the current scrape date
        next_scrape_date = str(pd.to_datetime(time_start) + pd.Timedelta(hours = 1))
        next_scrape_date = parser.parse(next_scrape_date).isoformat()

        # Update next scrape date for current token locally
        predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
        coinapi_pairs_df.loc[predicate, 'latest_scrape_date_orderbook'] = next_scrape_date

        # Return updated token metadata DF
        return coinapi_pairs_df
    
    # Deletes order book data from S3
    def __delete_order_book_data_from_s3(self):

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
    def __get_order_book_snapshot(self, coinapi_symbol_id, time_start):
        
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
        api_request_url = 'https://rest.coinapi.io/v1/orderbooks/{}/history?time_start={}&limit={}'.format(coinapi_symbol_id, time_start, 1)
        headers = {'X-CoinAPI-Key':Variable.get('coin_api_api_key')}
        
        try:
            response = r.get(url = api_request_url, headers = headers)
        except:
            return -1

        # Request successful
        if response.status_code == 200:
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
   
    def execute(self, context):
        """
        Gets CoinAPI order book snapshots for tokens in DESIRED_TOKENS and stores them in S3.

        Parameters:
            context - Airflow context object

        Returns:
            None
        """
        # Delete order book data potentially stored in S3 from previous task runs
        self.__delete_order_book_data_from_s3()
        
        # S3 key for token metadata (next scrape dates)
        key = 'eth_data/metadata/coinapi_pair_metadata.json'

        # Read token metadata from S3 and load it into a DataFrame
        coinapi_pairs_str = self.s3_connection.read_key(key = key, bucket_name = 'project-poseidon-data')
        coinapi_pairs_json = json.loads(coinapi_pairs_str)
        coinapi_pairs_df = pd.DataFrame(coinapi_pairs_json)

        # Filter out tokens we don't want to get order book data for
        base_quote_exchange = coinapi_pairs_df['asset_id_base'] + '_' + coinapi_pairs_df['asset_id_quote'] + '_' + coinapi_pairs_df['exchange_id']
        predicate = base_quote_exchange.isin(self.DESIRED_TOKENS)
        coinapi_pairs_df = coinapi_pairs_df[predicate]

        # For each token in DESIRED_TOKENS
        for i in range(len(coinapi_pairs_df)):

            # List of order book snapshots for current token
            order_book_snapshot_data = []

            while True:
                coinapi_pair = coinapi_pairs_df.iloc[i]

                print('{}) pair: {}/{} (exchange: {})'.format(i + 1, coinapi_pair['asset_id_base'], coinapi_pair['asset_id_quote'], coinapi_pair['exchange_id']))
                print()
                
                coinapi_symbol_id = coinapi_pair['exchange_id'] + '_' + 'SPOT' + '_' + coinapi_pair['asset_id_base'] + '_' + coinapi_pair['asset_id_quote']

                # Get the next date to scrape order book data for current token
                time_start = self.__get_next_start_date(coinapi_pair)

                # Get order book snapshot on time_start for current token
                latest_order_book_data_for_pair = self.__get_order_book_snapshot(coinapi_symbol_id = coinapi_symbol_id, time_start = time_start)
                
                # If request didn't succeed
                if type(latest_order_book_data_for_pair) == int:

                    # If we have exceeded our API key rate limits then update token metadata stored in
                    # S3, upload new data collected thus far to S3, and stop this tas
                    if latest_order_book_data_for_pair == 429:
                        self.__upload_new_order_book_data(order_book_snapshot_data)
                        self.__upload_updated_coinapi_metadata(coinapi_pairs_df)
                        return

                    # Else just proceed to the next token
                    else:
                        break

                # If request succeeded
                else:

                    # If request returned an empty response
                    if len(latest_order_book_data_for_pair) == 0:
                        print('No data returned for request... continuing to next pair.')
                        print()
                        break
                    
                    # If request returned a non-empty response
                    else:
                        print('Data returned for ({})... continuing to next request.'.format(time_start))
                        
                        # Add to list of order book snapshots for current token
                        order_book_snapshot_data.extend(latest_order_book_data_for_pair)

                        # Update metadata for this token locally
                        coinapi_pairs_df = self.__update_coinapi_metadata(
                            time_start,
                            coinapi_pairs_df,
                            coinapi_pair
                        )

            # Upload new order book data collected for current token to S3
            self.__upload_new_order_book_data(order_book_snapshot_data)

        # Update token metadata stored in S3
        self.__upload_updated_coinapi_metadata(coinapi_pairs_df)