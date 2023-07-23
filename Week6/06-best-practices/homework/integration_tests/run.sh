#!/usr/bin/env bash

cd "$(dirname "$0")"
export BUCKET_NAME="nyc-duration"
export INPUT_FILE_PATTERN="s3://${BUCKET_NAME}/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://${BUCKET_NAME}/out/{year:04d}-{month:02d}.parquet"
export S3_ENDPOINT_URL="http://localhost:4566/"


# Getting localstack up
docker-compose up -d 

sleep 1

# creating bucket    
aws s3 mb s3://${BUCKET_NAME} --endpoint-url ${S3_ENDPOINT_URL}

# run integration tests
pipenv run python integration_test.py 2022 1

RESULT=$?

if [ $RESULT -eq 0 ]; then
  echo INTEGRATION TEST SUCCESSFUL
else
  docker-compose logs
  echo INTEGRATION TEST FAILED
fi

docker-compose down