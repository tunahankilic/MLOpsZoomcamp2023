#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pickle
import pandas as pd
from pathlib import Path


FILEPATH = Path(__file__).parent.resolve()
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration-prediction-alexey/taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def prepare_data(input_data: pd.DataFrame, categorical: list):
    """
    Takes raw data as an input and prepares data for prediction
    """
    df = input_data.copy()

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df


def read_data(filename: pd.DataFrame, categorical: list):

    if S3_ENDPOINT_URL:
        options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
        }

        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)
    
    df = prepare_data(input_data=df, categorical=categorical)
    
    return df


def save_data(output_file: str, output_data: pd.DataFrame):
    
    df = output_data.copy()
    if S3_ENDPOINT_URL:
        options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
        }
        }      
        df.to_parquet(output_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
        )
    else:
        df.to_parquet(output_file,
        engine='pyarrow',
        index=False
        )



def main(year: int, month: int):

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)
    #input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    #output_file = f'output/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    
    model_path = os.path.join(FILEPATH, 'model.bin')
    with open(model_path, 'rb') as f_in:
        dv, lr = pickle.load(f_in)


    categorical = ['PULocationID', 'DOLocationID']

    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())


    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    save_data(output_file=output_file, output_data=df_result)

    """
    if os.path.exists("output/"):
        df_result.to_parquet(output_file, engine='pyarrow', index=False)
    else:
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname("output/"), exist_ok=True)
        df_result.to_parquet(output_file, engine='pyarrow', index=False)
    """




if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    main(year=year, month=month)
