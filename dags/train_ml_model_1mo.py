import duckdb
import joblib
import pandas as pd
import numpy as np
import lightgbm as lgb

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

# from datasets import (
#     FINAL_ML_FEATURES,
#     ML_MODELS
# )
FINAL_ML_FEATURES = Dataset("~/LocalData/data/ml_features")
ML_MODELS = Dataset("~/LocalData/data/ml_models")

def train_ml_model(exec_date):
    exec_date = pd.to_datetime(exec_date)
    month_year = exec_date.strftime('%Y-%m')
    # Check if it is the first day of the month
    if exec_date.day != 1:
        # Not the first day of the month, skip training
        print(f"Skipping training for {exec_date.strftime('%Y-%m-%d')}, not the first day of the month.")
        return

    # Load the ML features for the past 2 years, up until the last day of the previous month
    min_date = exec_date - pd.Timedelta(days=365 * 2)
    ml_features_query = f"""
    SELECT *
    FROM read_parquet('{FINAL_ML_FEATURES.uri}/date=*/.parquet', hive_partitioning=true)
    WHERE date >= '{min_date.strftime('%Y-%m-%d')}' AND date < '{exec_date.strftime('%Y-%m-%d')}'
    """
    ml_features = duckdb.sql(ml_features_query).df()
    ml_features['symbol_id'] = ml_features['symbol_id'].astype('category')
    ml_features['day_of_week'] = ml_features['day_of_week'].astype('category')
    ml_features['month'] = ml_features['month'].astype('category')
    ml_features['day_of_month'] = ml_features['day_of_month'].astype('category')
    ml_features = ml_features[~ml_features['close_futures'].isna()]
    ml_features.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Define the target variable
    for symbol in ml_features['symbol_id'].unique():
        symbol_mask = ml_features['symbol_id'] == symbol
        ml_features.loc[symbol_mask, 'futures_forward_returns_7'] = ml_features.loc[symbol_mask, 'futures_returns_7'].shift(-7)
    ml_features = ml_features.dropna(subset=['futures_forward_returns_7'])

    # Columns we need to drop before training the model
    ml_features_cols = ml_features.columns.tolist()
    forward_returns_cols = [col for col in ml_features_cols if 'forward_returns' in col]
    non_numeric_cols = [
        'asset_id_base','asset_id_base_x','asset_id_base_y', 
        'asset_id_quote','asset_id_quote_x', 'asset_id_quote_y', 
        'exchange_id','exchange_id_x','exchange_id_y'
    ]
    other_cols = [
        'open_spot', 'high_spot', 'low_spot', 'close_spot', 
        'open_futures', 'high_futures', 'low_futures', 'close_futures', 
        'time_period_end', 'date'
    ]
    num_cols = [col for col in ml_features_cols if 'num' in col and 'rz' not in col and 'zscore' not in col and 'percentile' not in col]
    dollar_cols = [col for col in ml_features_cols if 'dollar' in col and 'rz' not in col and 'zscore' not in col and 'percentile' not in col]
    delta_cols = [col for col in ml_features_cols if 'delta' in col and 'rz' not in col and 'zscore' not in col and 'percentile' not in col]
    other = [col for col in ml_features_cols if '10th_percentile' in col or '90th_percentile' in col]
    cols_to_drop = (
        forward_returns_cols +
        non_numeric_cols +
        other_cols +
        num_cols +
        dollar_cols +
        delta_cols +
        other
    )
    
    X_train = ml_features.drop(columns=cols_to_drop, axis=1, errors='ignore')
    y_train = ml_features['futures_forward_returns_7']
    
    params = {
        'objective': 'regression_l2',
        'lambda_l2': 5,
        'learning_rate': 0.1,
        'n_estimators': 100,
        'max_depth': 5,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'random_state': 19,
        'n_jobs': -1
    }
    ml_model = lgb.LGBMRegressor(**params)
    ml_model.fit(X_train, y_train)

    # Save the model
    model_path = f"{ML_MODELS.uri}/date={exec_date.strftime('%Y-%m-%d')}/ml_model.pkl"
    joblib.dump(ml_model, model_path)

with DAG(
    dag_id="train_ml_model_1mo",
    schedule=[FINAL_ML_FEATURES],
    catchup=False,
) as dag:
    train_model = PythonOperator(
        task_id="train_ml_model",
        python_callable=train_ml_model,
        op_kwargs={
            'exec_date': "{{ ds }}"
        },
    )
    finish = EmptyOperator(task_id="finish", outlets=[ML_MODELS])
    train_model >> finish
