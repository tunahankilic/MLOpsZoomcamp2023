import batch
import pandas as pd
from pandas import Timestamp
from datetime import datetime
from deepdiff import DeepDiff


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


def test_prepare_data():
    data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    categorical = ['PULocationID', 'DOLocationID']
    df = pd.DataFrame(data, columns=columns)

    actual_output = batch.prepare_data(input_data=df, categorical=categorical).to_dict(orient='records')


    expected_output = [
        {
            'PULocationID': '-1',
            'DOLocationID': '-1',
            'tpep_pickup_datetime': Timestamp('2022-01-01 01:02:00'),
            'tpep_dropoff_datetime': Timestamp('2022-01-01 01:10:00'),
            'duration': 8,
        },
        {
            'PULocationID': '1',
            'DOLocationID': '-1',
            'tpep_pickup_datetime': Timestamp('2022-01-01 01:02:00'),
            'tpep_dropoff_datetime': Timestamp('2022-01-01 01:10:00'),
            'duration': 8,
        },
        {
            'PULocationID': '1',
            'DOLocationID': '2',
            'tpep_pickup_datetime': Timestamp('2022-01-01 02:02:00'),
            'tpep_dropoff_datetime': Timestamp('2022-01-01 02:03:00'),
            'duration': 1,
        }
    ]

    assert actual_output == expected_output
