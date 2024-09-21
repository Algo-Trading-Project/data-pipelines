from airflow.models import BaseOperator
from airflow.models import Variable

import requests as r
import json
import pandas as pd
import dateutil.parser as parser
import duckdb

class GetTickDataOperator(BaseOperator):

    DESIRED_TOKENS = [
        'BTC_USD_COINBASE', 'ETH_USD_COINBASE', 'BNB_USDC_BINANCE', 
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __get_next_start_date(self, coinapi_pair):
        most_recent_data_date = coinapi_pair['latest_scrape_date_trade']
        next_start_date = None

        if pd.isnull(most_recent_data_date):
            next_start_date = parser.parse(str(coinapi_pair['data_orderbook_start'])).isoformat().split('+')[0].split('.')[0]
        else:
            next_start_date = parser.parse(str(most_recent_data_date)).isoformat().split('+')[0].split('.')[0]

        return next_start_date

    def __upload_new_tick_data(self, new_tick_data):
        # If there is no new tick data to upload, return
        if len(new_tick_data) == 0:
            return

        # Else upload new tick data to DuckDB

        # Create temporary file to store new tick data
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_tick_data.json'
        data_to_upload = pd.DataFrame(new_tick_data)
        data_to_upload.to_json(path, orient = 'records')

        # Connect to DuckDB
        with duckdb.connect(
            database = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/database.db',
            read_only = False
        ) as conn:
            
            # Load the new tick data into the database
            query = f"""
            INSERT OR REPLACE INTO market_data.tick_data (symbol_id, time_exchange, time_coinapi, uuid, price, size, taker_side)
            SELECT * FROM read_json_auto('{path}')
            """
            conn.sql(query)
            conn.commit()

    def __upload_coinapi_metadata(self, coinapi_pairs_df):
        coinapi_pairs_json = coinapi_pairs_df.to_json(orient = 'records')

        # Write the metadata to a local JSON file
        with open('/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_metadata.json', 'w') as f:
            json.dump(coinapi_pairs_df_json, f)

    def __update_coinapi_pairs_metadata(self, time_end, coinapi_pairs_df, coinapi_pair):
        asset_id_base = coinapi_pair['asset_id_base']
        asset_id_quote = coinapi_pair['asset_id_quote']
        exchange_id = coinapi_pair['exchange_id']

        # The next start date is time_end
        new_latest_scrape_date = time_end
        
        predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
        coinapi_pairs_df.loc[predicate, 'latest_scrape_date_trade'] = new_latest_scrape_date

        return coinapi_pairs_df
    
    def __get_latest_tick_data(self, coinapi_symbol_id, time_start):
        def format_response_data(response):
            exchange_id, symbol_id, asset_id_base, asset_id_quote = coinapi_symbol_id.split('_')

            for elem in response:
                elem['exchange_id'] = exchange_id
                elem['asset_id_base'] = asset_id_base
                elem['asset_id_quote'] = asset_id_quote

            return response

        api_request_url = 'https://rest.coinapi.io/v1/trades/{}/history?time_start={}&limit=100000'.format(coinapi_symbol_id, time_start)
        api_key =  Variable.get('coinapi_api_key')
        
        payload = {}
        headers = {'Accept': 'application/json', 'X-CoinAPI-Key': api_key}

        response = r.get(url = api_request_url, headers = headers, data = payload)

        if response.status_code == 200:
            response_json = response.json()
            formatted_response = format_response_data(response_json)
            return formatted_response
        
        # Bad Request -- There is something wrong with your request
        elif response.status_code == 400:
            print('Bad Request -- There is something wrong with your request')
            print(response.json())
            print()
            return response.status_code
        
        # Unauthorized -- Your API key is wrong
        elif response.status_code == 401:
            print('Unauthorized -- Your API key is wrong')
            print(response.json())
            print()
            return response.status_code
        
        # Forbidden -- Your API key doesnt't have enough privileges to access this resource
        elif response.status_code == 403:
            print("Forbidden -- Your API key doesn't have enough privileges to access this resource")
            print(response.json())
            print()
            return response.status_code
        
        # Too many requests -- You have exceeded your API key rate limits
        elif response.status_code == 429:
            print('Too many requests -- You have exceeded your API key rate limits')
            print(response.json())
            print()
            return response.status_code

        # No data -- You requested specific single item that we don't have at this moment.
        elif response.status_code == 550:
            print("No data -- You requested specific single item that we don't have at this moment")
            print(response.json())
            print()
            return response.status_code
        else:
            print('Unknown error')
            print(response.json())
            print()
            return -1
        
    def execute(self, context):
        # File path for token metadata (last scrape dates)
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_metadata.json'

        # Read token metadata from file and load it into a DataFrame
        f = open(path, 'r')
        coinapi_pairs_json = json.load(f)
        coinapi_pairs_df = pd.DataFrame(coinapi_pairs_json)

        # Shuffle the DataFrame to randomize the order of tokens
        coinapi_pairs_df = coinapi_pairs_df.sample(frac = 1).reset_index(drop = True)
        coinapi_pairs_df = coinapi_pairs_df[(coinapi_pairs_df['asset_id_base'] == 'BTC') & (coinapi_pairs_df['asset_id_quote'] == 'USD') & (coinapi_pairs_df['exchange_id'] == 'COINBASE')]

        # For each token in DESIRED_TOKENS
        for i in range(len(coinapi_pairs_df)):
            
            # Get token metadata for current token
            coinapi_token = coinapi_pairs_df.iloc[i]

            self.log.info('GetTickDataOperator: {}) token: {}/{} (exchange: {})'.format(i + 1, coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']))
            self.log.info('GetTickDataOperator: ')

            while True:

                # Get the symbol ID for the current token 
                coinapi_symbol_id = coinapi_token['exchange_id'] + '_' + 'SPOT' + '_' + coinapi_token['asset_id_base'] + '_' + coinapi_token['asset_id_quote']

                # Get the latest date that was scraped for this pair
                time_start = self.__get_next_start_date(coinapi_token)
                
                self.log.info('GetTickDataOperator: Getting tick data starting from {}...'.format(time_start))
                self.log.info('GetTickDataOperator: ')

                # Get new data since the latest scrape date
                latest_tick_data_for_token = self.__get_latest_tick_data(
                    coinapi_symbol_id = coinapi_symbol_id, 
                    time_start = time_start
                )                
                
                # If request didn't succeed
                if type(latest_tick_data_for_token) == int:

                    self.log.info('GetTickDataOperator: Request failed...')
                    self.log.info('GetTickDataOperator: ')

                    # If we have exceeded our API key rate limits then update token metadata
                    # and stop this task
                    if latest_tick_data_for_token == 429:
                        self.__upload_coinapi_metadata(coinapi_pairs_df)
                        return

                    # Else just proceed to the next token
                    else:
                        break

                # If request succeeded
                else:

                    # If request returned an empty response
                    if len(latest_tick_data_for_token) <= 1:
                        self.log.info(f'GetTickDataOperator: data returned for request: {latest_tick_data_for_token}')
                        self.log.info('GetTickDataOperator: No data returned for request... continuing to next token.')
                        self.log.info('GetTickDataOperator: ')
                        break
                    
                    # If request returned a non-empty response
                    else:
                        self.log.info('GetTickDataOperator: Request succeeded... Uploading tick data to DuckDB and updating token metadata.')
                        self.log.info('GetTickDataOperator: ')

                        # Get most recent date from the new data to use as the new latest scrape date
                        time_end = latest_tick_data_for_token[-1]['time_exchange']
                        
                        # Upload new tick data for this token to DuckDB
                        self.__upload_new_tick_data(latest_tick_data_for_token)

                        # Update token metadata for this pair locally
                        coinapi_pairs_df = self.__update_coinapi_pairs_metadata(
                            time_end,
                            coinapi_pairs_df,
                            coinapi_pair
                        )

                        # Update token metadata stored in DuckDB
                        self.__upload_coinapi_metadata(coinapi_pairs_df)