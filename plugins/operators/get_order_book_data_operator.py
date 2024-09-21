from airflow.models import BaseOperator
from airflow.models import Variable
from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as r
import json
import pandas as pd
import dateutil.parser as parser
import duckdb

import os
os.environ['NO_PROXY'] = '*'

class GetOrderBookDataOperator(BaseOperator):
    """
    Custom Airflow operator to fetch and store CoinAPI order book snapshots for specified tokens in Amazon S3.

    This operator retrieves order book data for a predefined list of tokens (`DESIRED_TOKENS`) and stores 
    the data in an S3 bucket. It includes mechanisms for handling failures, retries, and data synchronization.
    """
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # List of order book snapshots
        self.order_book_snapshots = []

    def _get_next_start_dates(self, coinapi_token):
            """
            Calculates the next set of start dates for scraping order book data for a given token.

            This method generates a list of the next ten valid start dates, considering the most recent scrape date. 
            It ensures that the dates are rounded to the nearest hour and are within the current UTC time.

            Parameters:
                coinapi_token (pandas.Series): A pandas Series containing metadata for a specific token.

            Returns:
                list of str: A list of ISO 8601 formatted start dates.
            """
            
            # Round pandas datetime object t up to the nearest 30 minutes
            def half_hour_rounder(t):
                rounded_half_hour = t.replace(second=0, microsecond=0, minute=(t.minute // 30) * 30) + timedelta(minutes=30)
                return rounded_half_hour.strftime('%Y-%m-%d %H:%M:%S')

            # Get the next date to scrape order book data for current token
            next_start_date = coinapi_token['latest_scrape_date_orderbook']

            # If we have never scraped order book data for this token before
            if pd.isnull(next_start_date):

                # Get the date of the first order book snapshot for this token
                next_start_date = pd.to_datetime(coinapi_token['data_orderbook_start'])

                # Round the date up to the nearest hour
                next_start_date = half_hour_rounder(next_start_date)

                # Convert the date to ISO 8601 format
                next_start_date = parser.parse(str(next_start_date)).isoformat().split('+')[0]

                # Get the next 10 valid start dates separated by 30 minutes
                next_start_dates = [parser.parse(next_start_date) + timedelta(minutes = 30 * i) for i in range(10)]
                next_start_dates = [date.isoformat().split('+')[0] for date in next_start_dates if date <= datetime.utcnow()]

            # If we have scraped order book data for this token before
            else:

                # Convert the date to ISO 8601 format
                next_start_date = parser.parse(str(next_start_date)).isoformat().split('+')[0]

                # Get the next 10 start dates separated by 30 minutes
                next_start_dates = [parser.parse(next_start_date) + timedelta(minutes = 30 * i) for i in range(10)]
                next_start_dates = [date.isoformat().split('+')[0] for date in next_start_dates if date <= datetime.utcnow()]

            return next_start_dates
 
    def _upload_new_order_book_data(self):
        """
        Uploads newly collected order book data to S3.

        This method converts the order book data to JSON format and uploads it to a specified S3 bucket. 
        It handles the process of chunking data and managing file names for storage.

        Returns:
            None
        """

        # If there is no new order book data to upload then return
        if len(self.order_book_snapshots) == 0:
            return
        
        # Else upload new order book data to DuckDB

        # Create temporary file to store order book data
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_order_book_data_30m.json'
        data_to_upload = pd.DataFrame(self.order_book_snapshots)
        data_to_upload.to_json(path, orient = 'records')

        # Connect to DuckDB
        with duckdb.connect(
            database = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/database.db',
            read_only = False
        ) as conn:
            
            # Load the new order book data into the database
            query = f"""
            INSERT OR REPLACE INTO market_data.order_book_snapshot_30m (symbol_id, time_exchange, time_coinapi, asks, bids)
            SELECT * FROM read_json_auto('{path}')
            """
            conn.sql(query)
            conn.commit()

        # Clear the list of order book snapshots
        self.order_book_snapshots = []

    def _upload_coinapi_metadata(self, coinapi_pairs_df):
        """
        Uploads updated token metadata to S3.

        Converts the updated token metadata DataFrame to JSON and uploads it to a specific S3 bucket. 
        This ensures that the metadata in S3 is always in sync with the latest scrape dates.

        Returns:
            None
        """

        coinapi_pairs_df_json = coinapi_pairs_df.to_dict(orient = 'records')

        # Write the metadata to a local JSON file
        with open('/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/coinapi_metadata.json', 'w') as f:
            json.dump(coinapi_pairs_df_json, f)

    def _update_coinapi_metadata(self, time_start, coinapi_token, coinapi_pairs_df):
            """
            Updates the next scrape date for a token in the local metadata.

            This method modifies the metadata DataFrame to reflect the next scrape date for a particular token, 
            based on the most recent successful data retrieval.

            Parameters:
                time_start (str): The start time of the most recent successful data retrieval.
                
                coinapi_token (pandas.Series): A pandas Series containing metadata for a specific token.

            Returns:
                None
            """

            asset_id_base = coinapi_token['asset_id_base']
            asset_id_quote = coinapi_token['asset_id_quote']
            exchange_id = coinapi_token['exchange_id']

            # Next scrape date is 30 minutes after the current scrape date
            next_scrape_date = str(pd.to_datetime(time_start) + pd.Timedelta(minutes = 30))
            next_scrape_date = parser.parse(next_scrape_date).isoformat()

            # Update next scrape date for current token locally
            predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
            coinapi_pairs_df.loc[predicate, 'latest_scrape_date_orderbook'] = next_scrape_date
    
    def _get_order_book_snapshot(self, coinapi_symbol_id, time_start):
        """
        Retrieves a single order book snapshot from CoinAPI for a given token and time.

        Sends a request to the CoinAPI to fetch the order book data for a specific cryptocurrency token at 
        a given time. Handles various HTTP response statuses to identify successful and failed requests.

        Parameters:
            coinapi_symbol_id (str): The identifier for the cryptocurrency symbol in CoinAPI.

            time_start (str): The start time for the data request in ISO 8601 format.

        Returns:
            list or int: A list of a single order book snapshot if successful, otherwise an error code.
        """

        # Formats response data from CoinAPI
        def format_response_data(response):
            exchange_id, symbol_id, asset_id_base, asset_id_quote = coinapi_symbol_id.split('_')
            
            for elem in response:
                elem['bids'] = json.dumps(elem['bids'])
                elem['asks'] = json.dumps(elem['asks'])
                elem['symbol_id'] = asset_id_base + '_' + asset_id_quote + '_' + exchange_id

            return response

        # Make request to CoinAPI
        api_key = Variable.get('coinapi_api_key')
        api_request_url = f'https://rest.coinapi.io/v1/orderbooks/{coinapi_symbol_id}/history?time_start={time_start}&limit={1}'
        
        payload = {}
        headers = {'Accept': 'application/json', 'X-CoinAPI-Key': api_key}
        
        try:
            response = r.get(url = api_request_url, headers = headers, data = payload)
        except:
            return -1

        # Request successful
        if response.status_code == 200:
            response_json = response.json()
            formatted_response = format_response_data(response_json)
            return formatted_response
        
        # Bad Request -- There is something wrong with your request
        elif response.status_code == 400:
            print('GetOrderBookDataOperator: Bad request: {}'.format(response.json()))
            return response.status_code
        
        # Unauthorized -- Your API key is wrong
        elif response.status_code == 401:
            print('GetOrderBookDataOperator: Unauthorized: {}'.format(response.json()))
            return response.status_code
        
        # Forbidden -- Your API key doesnt't have enough privileges to access this resource
        elif response.status_code == 403:
            print('GetOrderBookDataOperator: Forbidden: {}'.format(response.json()))
            return response.status_code
        
        # Too many requests -- You have exceeded your API key rate limits
        elif response.status_code == 429:
            print('GetOrderBookDataOperator: Too many requests: {}'.format(response.json()))
            return response.status_code

        # No data -- You requested specific single item that we don't have at this moment.
        elif response.status_code == 550:
            print('GetOrderBookDataOperator: No data: {}'.format(response.json()))
            return response.status_code
        
        # Unknown error
        else:
            print('GetOrderBookDataOperator: Unknown error: {}'.format(response.json()))
            return -1
   
    def _get_order_book_snapshots_concurrently(self, coinapi_symbol_id, start_times, max_retries = 1):
        """
        Concurrently retrieves order book snapshots from CoinAPI for a given cryptocurrency symbol 
        and a list of start times.

        This method manages concurrent requests to CoinAPI for multiple timestamps, 
        handling failures and retries. It's designed to efficiently gather large volumes of data.

        Parameters:
            coinapi_symbol_id (str): The symbol identifier for the cryptocurrency.

            start_times (list of str): Timestamps for which to get order book snapshots.

            max_retries (int, optional): Maximum number of retries for failed requests. Defaults to 1.

        Returns:
            list: A list of order book snapshots.
            set: A set of successful start times for which order book snapshots were collected.

        Raises:
            Exception: Specific exceptions are raised and logged if API requests fail after retries.
        """

        # List of order book snapshots
        results = []

        # Set of failed requests
        failed_requests = set()

        # Set of successful start times
        successful_start_times = set()

        with ThreadPoolExecutor(max_workers = 10) as executor:
            # Concurrently get order book snapshots for each start time

            # Submit the tasks and create a mapping of futures to api request timestamps
            futures = [executor.submit(self._get_order_book_snapshot, coinapi_symbol_id, start_time) for start_time in start_times]
            future_to_time = {future: start_time for future, start_time in zip(futures, start_times)}
            
            # As each future completes
            for future in as_completed(futures):

                # Get the api request timestamp for this future
                api_request_time = future_to_time[future]

                try:
                    # Get the result of the future
                    result = future.result()

                    # If the request failed then add it to the list of failed requests
                    if isinstance(result, int):
                        failed_requests.add(api_request_time)

                    # If the request succeeded then add the result to the list of results
                    else:
                        results.extend(result)

                        # Add the successful start time to the set of successful start times
                        successful_start_times.add(api_request_time)

                # If the request failed then add it to the list of failed requests
                except Exception as e:
                    failed_requests.add(api_request_time)

        # Retry logic for failed requests (without concurrency)

        # Set of succesfully retried requests
        succesfully_retried_requests = set()
        
        # Retry failed requests up to max_retries times
        for time in failed_requests:
            for _ in range(max_retries):
                try:
                    result = self._get_order_book_snapshot(coinapi_symbol_id, time)
                    if isinstance(result, int):
                        continue
                    else:
                        results.extend(result)
                        succesfully_retried_requests.add(time)
                        successful_start_times.add(time)
                        break

                except Exception as e:
                    continue 

        # Remove succesfully retried requests from failed requests (set difference)
        failed_requests = failed_requests - succesfully_retried_requests               

        for time in failed_requests:
            self.log.error('GetOrderBookDataOperator: Request for time ({}) failed after {} retries.'.format(time, max_retries))

        return results, successful_start_times
        
    def execute(self, context):
        """
        Main execution function for the GetOrderBookDataOperator.

        This method orchestrates the entire process of collecting and storing CoinAPI order book data for specified tokens. 
        It involves deleting existing data from S3, iterating over each token to collect new data, handling data synchronization 
        with S3, and ensuring continuous data retrieval until all required data points are collected or no more valid dates are available.

        Parameters:
            context (dict): The Airflow context object containing runtime information about the task.

        Returns:
            None
        """

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

            self.log.info('GetOrderBookDataOperator: {}) token: {}/{} (exchange: {})'.format(i + 1, coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']))
            self.log.info('GetOrderBookDataOperator: ')

            while True:

                # Every time we collect at least 24 * 30 = 720 (~1 month) order
                # book snapshots for current token
                if len(self.order_book_snapshots) >= 2 * 24 * 30:

                    self.log.info('GetOrderBookDataOperator: Collected ~1 month of order book snapshots for current token... uploading data to DuckDB.')
                    self.log.info('GetOrderBookDataOperator: ')

                    # Sync collected data and updated metadata with DuckDB and continue processing
                    self._upload_coinapi_metadata(coinapi_pairs_df)
                    self._upload_new_order_book_data()
                    continue

                # Get CoinAPI symbol ID for current token
                coinapi_symbol_id = coinapi_token['exchange_id'] + '_' + 'SPOT' + '_' + coinapi_token['asset_id_base'] + '_' + coinapi_token['asset_id_quote']

                # Get the next 10 valid dates to scrape order book data for current token
                next_start_dates = self._get_next_start_dates(coinapi_token)

                # If there are no more valid dates to scrape for current token
                if len(next_start_dates) == 0:

                    self.log.info('GetOrderBookDataOperator: No more valid dates to scrape for current token... continuing to next token.')
                    self.log.info('GetOrderBookDataOperator: ')

                    # Sync collected data and updated metadata with DuckDB and move on to next token
                    self._upload_coinapi_metadata(coinapi_pairs_df)
                    self._upload_new_order_book_data()
                    break

                self.log.info('GetOrderBookDataOperator: ******* Getting order book data from {} to {}'.format(next_start_dates[0], next_start_dates[-1]))
                self.log.info('GetOrderBookDataOperator: ')
            
                # Concurrently get next 10 order book snapshots for current token
                scraped_order_book_snapshots, successful_start_times = self._get_order_book_snapshots_concurrently(coinapi_symbol_id, next_start_dates)

                # Convert successful_start_times to datetime objects
                successful_start_times = [pd.to_datetime(time) for time in successful_start_times]
                
                # If request failed (no order book snapshots collected)
                if len(successful_start_times) == 0:

                    self.log.info('GetOrderBookDataOperator: Request failed... continuing to next token.')
                    self.log.info('GetOrderBookDataOperator: ')

                    # Sync collected data and updated metadata with DuckDB and move on to next token
                    self._upload_coinapi_metadata(coinapi_pairs_df)
                    self._upload_new_order_book_data()
                    break

                # If request succeeded (at least one order book snapshot collected)
                else:

                    print('GetOrderBookDataOperator: ({}/{}) order book snapshots collected for current token.'.format(len(successful_start_times), len(next_start_dates)))
                    print()

                    # Get the maximum date from the list of successful start times
                    max_date = max(successful_start_times)
                    
                    # Add to list of order book snapshots
                    self.order_book_snapshots.extend(scraped_order_book_snapshots)

                    # Update metadata for this token locally
                    self._update_coinapi_metadata(max_date, coinapi_token, coinapi_pairs_df)