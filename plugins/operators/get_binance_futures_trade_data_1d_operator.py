from airflow.models import BaseOperator
from datetime import timedelta, datetime
from airflow.models import Variable

import pandas as pd
import numpy as np
import zipfile
import io
import aiohttp
import asyncio
import pathlib
import pendulum
import duckdb
import os

class GetBinanceFuturesTradeDataDailyOperator(BaseOperator): 
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
 
    def _upload_new_futures_trade_data(self, futures_trade_data, time_start):
        data_to_upload = pd.DataFrame(futures_trade_data)
        symbol_id = f"{data_to_upload['asset_id_base'].iloc[0]}_{data_to_upload['asset_id_quote'].iloc[0]}_{data_to_upload['exchange_id'].iloc[0]}"
        data_to_upload['symbol_id'] = symbol_id
        date = time_start.strftime('%Y-%m-%d')
        
        s3_dir = f's3://base-44-data/data/futures_trade_data/raw/symbol_id={symbol_id}/date={date}/futures_trade_data.parquet'

        # Retrieve AWS credentials from Airflow Variables (Astronomer)
        aws_key = Variable.get("AWS_ACCESS_KEY_ID")
        aws_secret = Variable.get("AWS_SECRET_ACCESS_KEY")
        aws_region = Variable.get("AWS_DEFAULT_REGION")

        # Set environment for DuckDB (legacy auth scheme uses env vars)
        os.environ['AWS_ACCESS_KEY_ID'] = aws_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret
        os.environ['AWS_DEFAULT_REGION'] = aws_region
        
        # Register the data_to_upload DataFrame as a DuckDB table
        con = duckdb.connect(database=':memory:')
        con.register('data_to_upload', data_to_upload)

        # Use DuckDB to write the DataFrame to Parquet on S3
        con.execute(
            f"""
            COPY data_to_upload
            TO {s3_dir} (
                FORMAT 'PARQUET',
                COMPRESSION 'SNAPPY',
                OVERWRITE
            );
            """
        )

    def _write_to_log(self, path, data):
        # Ensure the directory exists or create it
        pathlib.Path(path).parent.mkdir(parents = True, exist_ok = True)
        with open(path, 'a') as f:
            f.write(data)

    async def _get_futures_trade_data(self, session, sem, time_start, coinapi_token, binance_metadata):
        year = time_start.year
        month = f'{time_start.month:02d}'
        day = f'{time_start.day:02d}'
        base, quote, exchange = coinapi_token['asset_id_base'], coinapi_token['asset_id_quote'], coinapi_token['exchange_id']
        url = f'https://data.binance.vision/data/futures/um/daily/trades/{base}{quote}/{base}{quote}-trades-{year}-{month}-{day}.zip'
        self.log.info(f'Retrieving futures data for {month}-{day}-{year} for {base}/{quote}...')

        try:
            async with sem:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=180)) as response:
                    if response.status != 200:
                        self.log.warning(f'Error retrieving data for {month}-{year}-{day}: {response.status} for {base}/{quote} from {url}')
                        # Log missing data to a file for later review
                        # log_path = pathlib.Path.home() / "LocalData" / "data" / "futures_trade_data" / "logs" / "error_log.log"
                        # symbol_id = f"{base}_{quote}_{exchange}"
                        # # date, symbol_id, HTTP status code (optional), Exception message (optional)
                        # row = f'{year}-{month}-{day},{symbol_id},{response.status},null' + '\n'
                        # self._write_to_log(log_path, row)
                        return

                    content = await response.read()

            with zipfile.ZipFile(io.BytesIO(content)) as z:
                with z.open(f'{base}{quote}-trades-{year}-{month}-{day}.csv') as f:
                    df = pd.read_csv(f, header = None)

        except Exception as e:
            self.log.warning(f'Error retrieving data for {month}-{year}-{day}: {str(e)} for {base}/{quote} from {url}')
            # Log exception to a file for later review
            # log_path = pathlib.Path.home() / "LocalData" / "data" / "futures_trade_data" / "logs" / "error_log.log"
            # symbol_id = f"{base}_{quote}_{exchange}"
            # # date, symbol_id, HTTP status code (optional), Exception message (optional)
            # row = f'{year}-{month}-{day},{symbol_id},null,{str(e)}' + '\n'
            # self._write_to_log(log_path, row)
            return

        if df.empty:
            self.log.info(f'No data found for {base}{quote} on {year}-{month}-{day}.')
            # Log missing data to a file for later review
            # log_path = pathlib.Path.home() / "LocalData" / "data" / "futures_trade_data" / "logs" / "error_log.log"
            # symbol_id = f"{base}_{quote}_{exchange}"
            # # date, symbol_id, HTTP status code (optional), Exception message (optional)
            # row = f'{year}-{month}-{day},{symbol_id},null,No data found' + '\n'
            # self._write_to_log(log_path, row)
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
                
        self._upload_new_futures_trade_data(df, time_start = time_start)
        
    def execute(self, context):
        # Load Binance metadata locally in Astronomer
        path = '/usr/local/airflow/include/binance_metadata.json'
        binance_metadata = pd.read_json(path, lines = True)
        execution_date = context['logical_date'].astimezone(pendulum.timezone('UTC'))
        target_date = (execution_date - pd.Timedelta(days = 1)).date()

        self.log.info(f'Starting futures trade data retrieval for {target_date}...')

        # Function to parallelize the download of daily futures trade data for all tokens
        async def _runner():
            sem = asyncio.Semaphore(5)
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