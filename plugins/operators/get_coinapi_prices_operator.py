from airflow.models import BaseOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import requests as r
import json
import pandas as pd
import dateutil.parser as parser

class GetCoinAPIPricesOperator(BaseOperator):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.s3_connection = S3Hook(aws_conn_id = 's3_conn')
        self.s3_file_chunk_num = 1

    def __get_next_start_date(self, coinapi_pair):
        most_recent_data_date = coinapi_pair['latest_scrape_date_price']
        next_start_date = None

        if pd.isnull(most_recent_data_date):
            next_start_date = parser.parse(str(coinapi_pair['data_start'])).isoformat().split('+')[0]
        else:
            next_start_date = parser.parse(str(most_recent_data_date)).isoformat().split('+')[0]

        return next_start_date

    def __upload_new_price_data(self, new_price_data):
        data_to_uplaod = json.dumps(new_price_data).replace('[', '').replace(']', '').replace('},', '}')
        key = 'eth_data/price_data/coinapi_pair_prices_1_hour.json.{}'.format(self.s3_file_chunk_num)

        self.s3_connection.load_string(
            string_data = data_to_uplaod, 
            key = key, 
            bucket_name = 'project-poseidon-data', 
            replace = True
        )

        self.s3_file_chunk_num += 1

    def __upload_new_coinapi_eth_pairs_metadata(self, coinapi_pairs_df):
        coinapi_pairs_df_json = coinapi_pairs_df.to_dict(orient = 'records')
        coinapi_pairs_str = json.dumps(coinapi_pairs_df_json)

        self.s3_connection.load_string(
            string_data = coinapi_pairs_str,
            key = 'eth_data/price_metadata/coinapi_pair_metadata.json',
            bucket_name = 'project-poseidon-data',
            replace = True
        )

    def __update_coinapi_pairs_metadata(self, latest_price_data_for_pair, coinapi_pairs_df, coinapi_pair):
        asset_id_base = coinapi_pair['asset_id_base']
        asset_id_quote = coinapi_pair['asset_id_quote']
        exchange_id = coinapi_pair['exchange_id']

        sort_key = lambda x: pd.to_datetime(x['time_period_start'])

        element_w_latest_date = max(latest_price_data_for_pair, key = sort_key)
        new_latest_scrape_date = str(pd.to_datetime(element_w_latest_date['time_period_start']) + pd.Timedelta(hours = 1))
        new_latest_scrape_date = parser.parse(new_latest_scrape_date).isoformat()

        predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
        coinapi_pairs_df.loc[predicate, 'latest_scrape_date_price'] = new_latest_scrape_date

        return coinapi_pairs_df
    
    def __delete_price_data_from_s3(self):
        keys_to_delete = self.s3_connection.list_keys(
            bucket_name = 'project-poseidon-data',
            prefix = 'eth_data/price_data'
        )

        self.s3_connection.delete_objects(
            bucket = 'project-poseidon-data', 
            keys = keys_to_delete
        )

    def __get_latest_price_data(self, coinapi_symbol_id, time_start):
        def format_response_data(response):
            exchange_id, symbol_id, asset_id_base, asset_id_quote = coinapi_symbol_id.split('_')
            
            for elem in response:
                elem['exchange_id'] = exchange_id
                elem['asset_id_base'] = asset_id_base
                elem['asset_id_quote'] = asset_id_quote

            return response

        api_request_url = 'https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1HRS&time_start={}&limit={}'.format(coinapi_symbol_id, time_start, 10000)
        headers = {'X-CoinAPI-Key':Variable.get('coin_api_api_key')}
        
        response = r.get(
            url = api_request_url,
            headers = headers,
        )

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
        else:
            return -1
        
    def execute(self, context):
        # Delete price data potentially stored in S3 from previous task runs
        self.__delete_price_data_from_s3()

        # S3 key for token metadata (last scrape dates)
        key = 'eth_data/price_metadata/coinapi_pair_metadata.json'

        # Read token metadata from S3 and load it into a DataFrame
        coinapi_pairs_str = self.s3_connection.read_key(key = key, bucket_name = 'project-poseidon-data')
        coinapi_pairs_json = json.loads(coinapi_pairs_str)
        coinapi_pairs_df = pd.DataFrame(coinapi_pairs_json)

        # For each token we have metadata for
        for i in range(len(coinapi_pairs_df)):
            while True:
                coinapi_pair = coinapi_pairs_df.iloc[i]

                print('{}) pair: {}/{} (exchange: {})'.format(i + 1, coinapi_pair['asset_id_base'], coinapi_pair['asset_id_quote'], coinapi_pair['exchange_id']))
                print()
                
                coinapi_symbol_id = coinapi_pair['exchange_id'] + '_' + 'SPOT' + '_' + coinapi_pair['asset_id_base'] + '_' + coinapi_pair['asset_id_quote']

                # Get the latest date that was scraped for this pair
                time_start = self.__get_next_start_date(coinapi_pair)

                # Get new data since the latest scrape date
                latest_price_data_for_pair = self.__get_latest_price_data(coinapi_symbol_id = coinapi_symbol_id, time_start = time_start)
                
                # If request didn't succeed
                if type(latest_price_data_for_pair) == int:

                    # If we have exceeded our API key rate limits then update token metadata stored in
                    # S3 and stop this task
                    if latest_price_data_for_pair == 429:
                        self.__upload_new_coinapi_eth_pairs_metadata(coinapi_pairs_df)
                        return

                    # Else just proceed to the next token
                    else:
                        break

                # If request succeeded
                else:

                    # If request returned an empty response
                    if len(latest_price_data_for_pair) == 0:
                        print('No data returned for request... continuing to next pair.')
                        print()
                        break
                    
                    # If request returned a non-empty response
                    else:
                        print('got data for this pair... uploading to S3 and updating coinapi pairs metadata.')
                        print()
                        
                        # Upload new price data for this pair to S3
                        self.__upload_new_price_data(latest_price_data_for_pair)

                        # Update token metadata for this pair locally
                        coinapi_pairs_df = self.__update_coinapi_pairs_metadata(
                            latest_price_data_for_pair,
                            coinapi_pairs_df,
                            coinapi_pair
                        )

        # Update token metadata stored in S3
        self.__upload_new_coinapi_eth_pairs_metadata(coinapi_pairs_df)