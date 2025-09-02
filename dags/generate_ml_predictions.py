import duckdb
import joblib
import pandas as pd
import numpy as np

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

# from datasets import (
#     FINAL_ML_FEATURES,
#     ML_PREDICTIONS,
#     ML_MODELS
# )

FINAL_ML_FEATURES = Dataset("~/LocalData/data/ml_features")
ML_PREDICTIONS = Dataset("~/LocalData/data/ml_predictions")
ML_MODELS = Dataset("~/LocalData/data/ml_models")

def generate_predictions(exec_date):
    exec_date = pd.to_datetime(exec_date)
    month_year = exec_date.strftime('%Y-%m')
    ml_features_query = f"""
    SELECT *
    FROM read_parquet('{FINAL_ML_FEATURES.uri}/date={exec_date.strftime('%Y-%m-%d')}/*.parquet', hive_partitioning=true)
    """
    ml_features = duckdb.sql(ml_features_query).df()
    ml_features_cols = ml_features.columns.tolist()

    # Columns we need to drop before training the model
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

    ml_model = joblib.load(f"{ML_MODELS.uri}/date={exec_date.strftime('%Y-%m-%d')}/ml_model.pkl")
    ml_model.set_params(verbosity=-1)
    input_features = ml_model.feature_names_in_

    ml_features = ml_features[input_features]
    ml_features['symbol_id'] = ml_features['symbol_id'].astype('category')
    ml_features['day_of_week'] = ml_features['day_of_week'].astype('category')
    ml_features['month'] = ml_features['month'].astype('category')
    ml_features['day_of_month'] = ml_features['day_of_month'].astype('category')
    ml_features = ml_features[~ml_features['close_futures'].isna()]
    ml_features.replace([np.inf, -np.inf], np.nan, inplace=True)
    ml_features = ml_features.drop(columns=cols_to_drop, errors='ignore')

    # Make predictions
    predictions = ml_model.predict(ml_features)
    ml_features['predicted_return_7d'] = predictions
    ml_features['date'] = exec_date.strftime('%Y-%m-%d')

    # Save predictions 
    cols_to_save = ['symbol_id', 'date', 'predicted_return_7d']
    ml_features[cols_to_save].to_csv(
        f"{ML_PREDICTIONS.uri}/date={exec_date.strftime('%Y-%m-%d')}/ml_predictions.csv",
        index=False
    )

with DAG(
    dag_id="generate_ml_predictions",
    schedule=[FINAL_ML_FEATURES, ML_MODELS],
    catchup=False,
) as dag:
    predict = PythonOperator(
        task_id="generate_predictions",
        python_callable=generate_predictions,
        op_kwargs={
            'exec_date': "{{ ds }}"
        },
    )
    finish = EmptyOperator(task_id="finish", outlets=[ML_PREDICTIONS])
    predict >> finish
