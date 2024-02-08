

import json
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from time import sleep
from typing import Tuple, List

import boto3
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator
from requests_aws4auth import AWS4Auth


start_time = datetime.now()

LambdaLoggerGenerator.remove_default_handlers()
logger = LambdaLoggerGenerator.get_logger(
    __name__,
    log_format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)
LambdaLoggerGenerator.get_logger('elasticsearch', log_level=logging.WARNING)

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
AWS_SESSION_TOKEN = os.environ.get('AWS_SESSION_TOKEN', None)
AWS_PROFILE = os.environ.get('AWS_PROFILE', None)
if (AWS_ACCESS_KEY_ID is None or AWS_SECRET_ACCESS_KEY is None) and AWS_PROFILE is None:
    logger.error('AWS credentials are not set. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.')
    exit(1)

OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT', None)
OPENSEARCH_PORT = os.environ.get('OPENSEARCH_PORT', 443)
OPENSEARCH_INDEX = os.getenv('OPENSEARCH_INDEX')
if OPENSEARCH_ENDPOINT is None or OPENSEARCH_PORT is None or OPENSEARCH_INDEX is None:
    logger.error('OpenSearch parameters are not set. Please set OPENSEARCH_ENDPOINT, OPENSEARCH_PORT, and '
                 'OPENSEARCH_INDEX.')
    exit(1)

S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_PREFIX', '').lstrip('/')

if S3_BUCKET is None:
    logger.error('S3_BUCKET parameter not set.')
    exit(1)

try:
    MAX_ES_WORKERS = int(os.getenv('MAX_ES_WORKERS'))
    assert MAX_ES_WORKERS > 0
except:
    MAX_ES_WORKERS = None

aws_session_params = dict()
if AWS_PROFILE:
    aws_session_params['profile_name'] = AWS_PROFILE
else:
    aws_session_params['aws_access_key_id'] = AWS_ACCESS_KEY_ID
    aws_session_params['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY

    if AWS_SESSION_TOKEN:
        aws_session_params['aws_session_token'] = AWS_SESSION_TOKEN

aws_session = boto3.Session(**aws_session_params)

aws_auth = AWS4Auth(
    aws_session.get_credentials().access_key,
    aws_session.get_credentials().secret_key,
    aws_session.region_name,
    'es',
    session_token=aws_session.get_credentials().token
)

opensearch_client = Elasticsearch(
    hosts=[{'host': OPENSEARCH_ENDPOINT, 'port': OPENSEARCH_PORT}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

s3 = aws_session.client('s3')

paginator = s3.get_paginator('list_objects_v2')
list_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)


def get_es_docs_for_url(url: str) -> Tuple[str, List[Tuple[str, int]]]:
    records = []

    retries = 3
    retry_delay = 1.5

    while retries > 0:
        try:
            records = []

            scroll = helpers.scan(
                client=opensearch_client,
                scroll='2m',
                index=OPENSEARCH_INDEX,
                query={"query": {"match": {"s3_url": url}}}
            )

            for doc in scroll:
                records.append(doc)

            break
        except Exception as e:
            logger.error(f'An exception occurred: {e}')
            retries -= 1
            sleep(retry_delay)
            retry_delay *= 2

    return url, records


uningested_urls = []

with ThreadPoolExecutor(max_workers=MAX_ES_WORKERS) as pool:
    total_checked = 0

    for page in list_iterator:
        futures = []
        contents = [o for o in page.get('Contents', []) if o['Key'].endswith('.json') or o['Key'].endswith('.json.gz')]

        total_checked += len(contents)
        logger.info(f'Checking {len(contents):,} keys ({total_checked:,} total)')

        for obj in contents:
            futures.append(pool.submit(get_es_docs_for_url, f's3://{S3_BUCKET}/{obj["Key"]}'))

        for result in as_completed(futures):
            s3_url, records = result.result()

            if len(records) == 0:
                uningested_urls.append(s3_url)

if len(uningested_urls) == 0:
    logger.info('No uningested files found')
else:
    logger.info(f'Found {len(uningested_urls):,} uningested files')

    filename = 'result'

    if os.path.exists(f'{filename}.json'):
        i = 0

        while os.path.exists(f'{filename}-{i}.json'):
            i += 1

        filename = f'{filename}-{i}'

    with open(f'{filename}.json', 'w') as fp:
        json.dump(
            dict(
                s3_url=f's3://{S3_BUCKET}/{S3_PREFIX}',
                total_checked=total_checked,
                total_uningested=len(uningested_urls),
                uningested_urls=uningested_urls
            ),
            fp,
            indent=4
        )

