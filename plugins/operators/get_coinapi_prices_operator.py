from airflow.models import BaseOperator
from airflow.models import Variable

import requests as r
import json
import pandas as pd
import dateutil.parser as parser
import duckdb

import os
os.environ['NO_PROXY'] = '*'

class GetCoinAPIPricesOperator(BaseOperator):
    """
    An Airflow operator to fetch and update Ethereum (ETH) token price data from CoinAPI,
    and manage related metadata.

    It performs several key functions:
    - Fetches latest price data from CoinAPI for different ETH pairs.
    - Uploads this data to DuckDB.
    - Updates and manages the metadata associated with each ETH pair.
    - Handles various API response statuses and potential errors.
    """

    # TODO: Refactor to use DuckDB
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __get_next_start_date(self, coinapi_pair):
        """
        Determines the next start date for fetching price data.

        Parameters:
        coinapi_pair: A dictionary containing information about the ETH pair.

        Returns:
        A string representing the next start date in ISO format.
        """

        most_recent_data_date = coinapi_pair['latest_scrape_date_price']
        next_start_date = None

        if pd.isnull(most_recent_data_date):
            next_start_date = parser.parse(str(coinapi_pair['data_start'])).isoformat().split('+')[0]
        else:
            next_start_date = parser.parse(str(most_recent_data_date)).isoformat().split('+')[0]

        return next_start_date

    def __upload_new_price_data(self, new_price_data):
        """
        Uploads new price data to DuckDB.

        Parameters:
        new_price_data: A list of dictionaries containing the new price data.
        """
        # Create a temporary json file to store the new price data
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_price_data_1m.json'
        with open(f'{path}', 'w') as f:
            json.dump(new_price_data, f)

        # Connect to the DuckDB database
        with duckdb.connect(
            database = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/database.db',
            read_only = False
        ) as conn:
            
            # Load the new price data into the database
            conn.sql(f"COPY market_data.price_data_1m FROM '{path}' (FORMAT JSON, AUTO_DETECT true)")
            conn.commit()

    def __upload_new_coinapi_eth_pairs_metadata(self, coinapi_pairs_df):
        """
        Writes the metadata of CoinAPI tokens to a local JSON file.

        Parameters:
        coinapi_pairs_df: A DataFrame containing the metadata of CoinAPI ETH pairs.
        """

        coinapi_pairs_df_json = coinapi_pairs_df.to_dict(orient = 'records')

        # Write the metadata to a local JSON file
        with open('/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_metadata.json', 'w') as f:
            json.dump(coinapi_pairs_df_json, f)

    def __update_coinapi_pairs_metadata(self, latest_price_data_for_pair, coinapi_pairs_df, coinapi_pair):
        """
        Updates the metadata DataFrame for a specific ETH pair.

        Parameters:
        latest_price_data_for_pair: Latest price data for the ETH pair.
        coinapi_pairs_df: DataFrame containing metadata for all ETH pairs.
        coinapi_pair: Information about the specific ETH pair.

        Returns:
        Updated DataFrame with the new metadata for the ETH pairs.
        """

        asset_id_base = coinapi_pair['asset_id_base']
        asset_id_quote = coinapi_pair['asset_id_quote']
        exchange_id = coinapi_pair['exchange_id']

        element_w_latest_date = latest_price_data_for_pair[-1]
        new_latest_scrape_date = str(pd.to_datetime(element_w_latest_date['time_period_start']) + pd.Timedelta(minutes = 1))
        new_latest_scrape_date = parser.parse(new_latest_scrape_date).isoformat()

        predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
        coinapi_pairs_df.loc[predicate, 'latest_scrape_date_price'] = new_latest_scrape_date

        return coinapi_pairs_df
    
    def __get_latest_price_data(self, coinapi_symbol_id, time_start):
        """
        Fetches the latest price data from CoinAPI.

        Parameters:
        coinapi_symbol_id: The symbol ID used in CoinAPI to identify the ETH pair.
        time_start: The start time from which to fetch the price data.

        Returns:
        Formatted response data as a list of dictionaries, or an error code as an integer.
        """

        def format_response_data(response):
            try:
                exchange_id, symbol_id, asset_id_base, asset_id_quote = coinapi_symbol_id.split('_')
            except:
                print('Error splitting {coinapi_symbol_id}')
                return -1
            
            for elem in response:
                elem['exchange_id'] = exchange_id
                elem['asset_id_base'] = asset_id_base
                elem['asset_id_quote'] = asset_id_quote

            return response

        api_request_url = 'https://rest.coinapi.io/v1/ohlcv/{}/history?period_id=1MIN&time_start={}&limit={}'.format(coinapi_symbol_id, time_start, 100000)
        headers = {'X-CoinAPI-Key':Variable.get('coinapi_api_key')}

        self.log.info('API request URL: {}'.format(api_request_url))
        
        try:
            response = r.get(
                url = api_request_url,
                headers = headers,
            )
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
            return response.status_code
        else:
            print('Unknown error occurred... continuing to next token.')
            print('API call response: ', response.json())
            print()
            return -1
        
    def execute(self, context):
        """
        The main execution function of the operator. 

        It orchestrates the entire process of data fetching, processing, and updating.
        This involves deleting existing price data from S3, reading and updating metadata
        for ETH pairs, and fetching new price data for each pair, followed by uploading it to S3.

        Parameters:
        context: Airflow's execution context object.
        """

        # File path for token metadata (last scrape dates)
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_metadata.json'

        # Read token metadata from file and load it into a DataFrame
        f = open(path, 'r')
        coinapi_pairs_json = json.load(f)
        coinapi_pairs_df = pd.DataFrame(coinapi_pairs_json)

        # For each token we have metadata for
        for i in range(len(coinapi_pairs_df)):
            while True:
                coinapi_pair = coinapi_pairs_df.iloc[i]

                self.log.info('{}) pair: {}/{} (exchange: {})'.format(i + 1, coinapi_pair['asset_id_base'], coinapi_pair['asset_id_quote'], coinapi_pair['exchange_id']))
                
                coinapi_symbol_id = coinapi_pair['symbol_id']

                # Get the latest date that was scraped for this pair
                time_start = self.__get_next_start_date(coinapi_pair)

                # Get new data since the latest scrape date
                latest_price_data_for_pair = self.__get_latest_price_data(coinapi_symbol_id = coinapi_symbol_id, time_start = time_start)
                
                # If request didn't succeed
                if type(latest_price_data_for_pair) == int:

                    # If we have exceeded our API key rate limits then update token metadata and stop this task
                    if latest_price_data_for_pair == 429:
                        self.log.info('API key rate limits exceeded... stopping task.')
                        self.__upload_new_coinapi_eth_pairs_metadata(coinapi_pairs_df)
                        return

                    # Else just proceed to the next token
                    else:
                        print('API request for data failed... continuing to next token.')
                        print()
                        break

                # If request succeeded
                else:

                    # If request returned an empty response
                    if len(latest_price_data_for_pair) == 0:
                        print('No data returned for request... continuing to next token.')
                        print()
                        break
                    
                    # If request returned a non-empty response
                    else:
                        self.log.info('got data for this token... uploading to database and updating coinapi tokens metadata.')
                        
                        # Upload new price data for this token to database
                        self.__upload_new_price_data(latest_price_data_for_pair)

                        # Update token metadata for this pair locally
                        coinapi_pairs_df = self.__update_coinapi_pairs_metadata(
                            latest_price_data_for_pair,
                            coinapi_pairs_df,
                            coinapi_pair
                        )
                        self.__upload_new_coinapi_eth_pairs_metadata(coinapi_pairs_df)