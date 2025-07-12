from airflow.models import BaseOperator
from datetime import timedelta, datetime

import pandas as pd
import numpy as np
import zipfile
import io
import aiohttp
import asyncio
import pathlib

class GetBinanceFuturesTradeDataOperator(BaseOperator): 
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_next_start_date(self, coinapi_token):                
            # Get the next start date for the current token
            next_start_date = pd.to_datetime(coinapi_token['futures_trade_data_end'], unit = 'ms') 

            # If there is no next start date, use the token's initial start date
            if pd.isnull(next_start_date):
                return pd.to_datetime(coinapi_token['futures_trade_data_start'], unit = 'ms')
            else:
                return next_start_date    
 
    def _upload_new_futures_trade_data(self, futures_trade_data, time_start):
        data_to_upload = pd.DataFrame(futures_trade_data)
        date = time_start.strftime('%Y-%m-%d')
        symbol_id = futures_trade_data['asset_id_base'].iloc[0] + '_' + futures_trade_data['asset_id_quote'].iloc[0] + '_' + futures_trade_data['exchange_id'].iloc[0]
        output_path = f'/Users/louisspencer/LocalData/data/futures_trade_data/symbol_id={symbol_id}/date={date}/futures_trade_data.parquet'
        data_to_upload.to_parquet(output_path, index = False, compression = 'snappy')

    def _update_coinapi_metadata(self, next_start_date, coinapi_token, coinapi_pairs_df):
            asset_id_base = coinapi_token['asset_id_base']
            asset_id_quote = coinapi_token['asset_id_quote']
            exchange_id = coinapi_token['exchange_id']

            # Update next scrape date for current token locally
            predicate = (coinapi_pairs_df['exchange_id'] == exchange_id) & (coinapi_pairs_df['asset_id_base'] == asset_id_base) & (coinapi_pairs_df['asset_id_quote'] == asset_id_quote)
            coinapi_pairs_df.loc[predicate, 'futures_trade_data_end'] = next_start_date

            # Write the metadata to a local JSON file
            metadata_path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
            coinapi_pairs_df.to_json(metadata_path, orient = 'records', lines = True)

    async def _get_futures_trade_data(self, session, sem, time_start, coinapi_token, binance_metadata):
        year = time_start.year
        month = f'{time_start.month:02d}'
        day = f'{time_start.day:02d}'
        base, quote, exchange = coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']
        url = f'https://data.binance.vision/data/futures/um/daily/trades/{base}{quote}/{base}{quote}-trades-{year}-{month}-{day}.zip'
        print(f'Getting futures trade data for {base}{quote} from {url}....')
        print()

        try:
            async with sem:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=180)) as response:
                    if response.status != 200:
                        print(f'Error retrieving data from {url}: {response.status}')
                        print(f'URL: {url}')
                        print(f'Response: {await response.text()}')
                        print()
                        return
                    content = await response.read()
        except Exception as e:
            print(f'Error retrieving data for {month}-{year}-{day}: {e}')
            print(f'URL: {url}')
            print()
            return

        with zipfile.ZipFile(io.BytesIO(content)) as z:
            with z.open(f'{base}{quote}-trades-{year}-{month}-{day}.csv') as f:
                df = pd.read_csv(
                    f, 
                    header = None
                )
                if df.empty:
                    print(f'No data found for {base}{quote} on {year}-{month}-{day}.')
                    return
                df.columns = ['trade_id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']
                df['side'] = np.where(df['is_buyer_maker'] == True, 'sell', 'buy')
                try:
                    df['time'] = pd.to_datetime(df['time'], unit = 'ms')
                except Exception as e:
                    df = df.iloc[1:]  # Skip the first row if it contains headers or incorrect data
                    df['time'] = pd.to_datetime(df['time'], unit = 'ms')

                df['asset_id_base'] = base
                df['asset_id_quote'] = quote
                df['exchange_id'] = exchange
                df['trade_id'] = df['trade_id'].astype(int)
                df['price'] = df['price'].astype(float)
                df['qty'] = df['qty'].astype(float)
                df['quote_qty'] = df['quote_qty'].astype(float)

                df = df.drop(columns = ['is_buyer_maker'])
                df = df.rename(columns = {'time': 'timestamp', 'qty': 'quantity', 'quote_qty': 'quote_quantity'})
                df = df[['trade_id', 'timestamp', 'price', 'quantity', 'quote_quantity', 'side', 'asset_id_base', 'asset_id_quote', 'exchange_id']].drop_duplicates(subset = ['trade_id'])
                
        print(df.head())
        print()
        max_date = df['timestamp'].max()

        self._upload_new_futures_trade_data(df, time_start = time_start)
        # self._update_coinapi_metadata(next_start_date = max_date, coinapi_token = coinapi_token, coinapi_pairs_df = binance_metadata)
        
    def execute(self, context):
        path = '/Users/louisspencer/Desktop/Trading-Bot-Data-Pipelines/data/binance_metadata.json'
        binance_metadata = pd.read_json(path, lines = True)
        execution_date = context['execution_date']
        target_date = (execution_date - timedelta(days = 1)).date()
        self.log.info(f'GetBinanceFuturesTradeDataOperator: Target date for futures trade data: {target_date}')
        self.log.info('GetBinanceFuturesTradeDataOperator: ')
        
        async def _runner():
            sem = asyncio.Semaphore(8)
            async with aiohttp.ClientSession() as session:
                tasks = []
                for i in range(len(binance_metadata)):
                    tasks.append(
                        self._get_futures_trade_data(
                            session, 
                            sem, 
                            target_date,
                            binance_metadata.iloc[i],
                            binance_metadata
                        )
                    )
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for e in [r for r in results if isinstance(r, Exception)]:
                    self.log.warning(f'Error in task: {e}')
        asyncio.run(_runner())