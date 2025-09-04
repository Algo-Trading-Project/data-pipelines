import duckdb
import pandas as pd
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from sklearn.pipeline import Pipeline
from analysis.ml.custom_transformers import *

def generate_ml_features(exec_date):
    exec_date = pd.to_datetime(exec_date)
    prev_date = (exec_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    cutoff = (exec_date - timedelta(days=365)).date()

    RAW_SPOT_OHLCV = '~/LocalData/data/ohlcv_data/raw'
    AGG_SPOT_OHLCV = '~/LocalData/data/ohlcv_data/agg'
    AGG_FUTURES_OHLCV = '~/LocalData/data/futures_ohlcv_data/agg'
    ROLLING_SPOT_TRADES = '~/LocalData/data/trade_data/agg'
    FINAL_ML_FEATURES = '~/LocalData/data/ml_features'
    
    tokens_with_data = f"""
    SELECT DISTINCT symbol_id
    FROM read_parquet('{AGG_SPOT_OHLCV}/symbol_id=*/date={prev_date}/*.parquet', hive_partitioning=true)
    WHERE symbol_id IN (
        SELECT symbol_id
        FROM read_parquet('{ROLLING_SPOT_TRADES}/symbol_id=*/date={prev_date}/*.parquet', hive_partitioning=true)
    """
    tokens_with_data = duckdb.sql(tokens_with_data).df()
    tokens_with_data = tokens_with_data['symbol_id'].tolist()
    
    ohlcv_spot = f"""
    SELECT *
    FROM read_parquet('{AGG_SPOT_OHLCV}/symbol_id=*/date=*/*.parquet', hive_partitioning=true)
    WHERE 
        symbol_id IN ({','.join(tokens_with_data)}) AND
        date BETWEEN '{cutoff}' AND '{prev_date}'
    ORDER BY symbol_id, date
    """
    ohlcv_spot = duckdb.sql(ohlcv_spot).df()

    ohlcv_futures = f"""
    SELECT *
    FROM read_parquet('{AGG_FUTURES_OHLCV}/symbol_id=*/date=*/*.parquet', hive_partitioning=true)
    WHERE 
        symbol_id IN ({','.join(tokens_with_data)}) AND
        date BETWEEN '{cutoff}' AND '{prev_date}'
    ORDER BY symbol_id, date
    """
    ohlcv_futures = duckdb.sql(ohlcv_futures).df()

    # Combine the dataframes
    merged = pd.merge(
        ohlcv_spot, 
        ohlcv_futures, 
        on=['symbol_id', 'date'], 
        how='left', 
        suffixes=('_spot', '_futures')
    ).sort_values(by=['symbol_id', 'date']).reset_index(drop=True)
    merged.rename(columns={
        'asset_id_base_spot': 'asset_id_base',
        'asset_id_quote_spot': 'asset_id_quote',
        'exchange_id_spot': 'exchange_id',
        'symbol_id_spot': 'symbol_id',
        'date_spot': 'date',
    }, inplace=True)
    merged = merged.drop(columns=['asset_id_base_futures', 'asset_id_quote_futures', 'exchange_id_futures', 'symbol_id_futures', 'date_futures'])

    # Pipeline for feature engineering and modeling
    feature_engineering_pipeline = Pipeline([
        ('time_features', TimeFeatures()),

        ('returns_features', ReturnsFeatures(
            window_sizes = [1, 7],
            lookback_windows = [30]
        )),

        ('risk_features', RiskFeatures(
            windows = [1, 7],
            lookback_windows = [30]
        )),

        ('trade_features', TradeFeatures(
            windows = [1, 7],
            lookback_windows = [30]
        )),

        ('spot_futures_features', SpotFuturesInteractionFeatures(
            windows = [1, 7],
            lookback_windows = [30]
        )),

        ('rolling_z_score', RollingZScoreScaler(window_sizes = [30]))
    ])

    final_features = []
    
    # For each individual symbol_id, pass through feature engineering pipeline
    for symbol_id in merged['symbol_id'].unique():
        symbol_data = merged[merged['symbol_id'] == symbol_id]
        assert symbol_data['date'].is_monotonic_increasing, "Input data is not sorted by date for symbol_id {}".format(symbol_id)
        features = feature_engineering_pipeline.fit_transform(symbol_data)
        assert features['date'].is_monotonic_increasing, "Output data is not sorted by date for symbol_id {}".format(symbol_id)
        final_features.append(features)

    final_features = pd.concat(final_features, ignore_index=True)
    # Register the final features in DuckDB and copy to FINAL_ML_FEATURES dataset as hive partitioned parquet files
    with duckdb.connect(':memory::') as duckdb:
        duckdb.register('final_ml_features', final_features)
        duckdb.sql(
            f"""
            COPY final_ml_features TO '{FINAL_ML_FEATURES}' (
                FORMAT PARQUET, 
                COMPRESSION 'SNAPPY',
                PARTITION_BY (date)
            )
            """
        )

start_date = pendulum.datetime(
    year=2018,
    month=1,
    day=1,
    tz='America/Los_Angeles'
)

with DAG(
    dag_id="generate_final_ml_features",
    start_date=start_date,
    schedule='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    compute = PythonOperator(
        task_id="compute_final_ml_features",
        python_callable=generate_ml_features,
        op_kwargs={
            'exec_date': "{{ ds }}"
        },
    )
    finish = EmptyOperator(task_id="finish")
    compute >> finish
