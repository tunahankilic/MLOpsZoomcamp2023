import os
import sys
import boto3
import subprocess
import pandas as pd
from math import isclose
from datetime import datetime


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
INPUT_FILE_PATTERN = os.getenv("INPUT_FILE_PATTERN", None)
OUTPUT_FILE_PATTERN = os.getenv("OUTPUT_FILE_PATTERN", None)
BUCKET_NAME = os.getenv("BUCKET_NAME", "nyc-duration")


def dt(hour, minute, second=0):
    """Function to prepare datetime object"""

    return datetime(2022, 1, 1, hour, minute, second)


def test_data_prep(year: int, month: int):
    
    data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df_input = pd.DataFrame(data, columns=columns)
    input_file = INPUT_FILE_PATTERN.format(year=year, month=month)
    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
            }
            }
    df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)


def test_save_data(year: int, month: int):

    subprocess.run(['python ../batch.py %s %s' % (year, month)], shell=True)
    client = boto3.client('s3', endpoint_url=S3_ENDPOINT_URL)
    response = client.head_object(Bucket = BUCKET_NAME, Key=f'in/{year:04d}-{month:02d}.parquet')

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200


def test_predictions(year:int, month:int):
    options = {
        'client_kwargs': {
            'endpoint_url': S3_ENDPOINT_URL
            }
            }
    output_file = OUTPUT_FILE_PATTERN.format(year=year, month=month)
    #print(f'output file: {output_file}')
    df = pd.read_parquet(output_file, storage_options=options)
    #print(df.head())
    actual_predictions = df['predicted_duration'].sum()
    print(f'actual predictions: {actual_predictions}')
    expected_predictions = 31.51

    assert isclose(actual_predictions, expected_predictions, abs_tol=10**-1)



if __name__ == "__main__":

    year = sys.argv[1]
    month = sys.argv[2]
    test_data_prep(year=int(year), month=int(month))
    test_save_data(year=int(year), month=int(month))
    test_predictions(year=int(year), month=int(month))