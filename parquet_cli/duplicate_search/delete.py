import json
import logging
import os
from datetime import datetime
from urllib.parse import urlparse

import boto3
from parquet_flask.cdms_lambda_func.lambda_logger_generator import LambdaLoggerGenerator

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

OPENSEARCH_PARQUET_PREFIX = os.getenv('OPENSEARCH_PARQUET_PREFIX')
if OPENSEARCH_PARQUET_PREFIX is None:
    logger.error('OPENSEARCH_PARQUET_PREFIX not set.')
    exit(1)

with open('search_result.json') as fp:
    search_results = json.load(fp)
    duplicated_jobs = search_results['confirmed_duplicates']

aws_session_params = dict()
if AWS_PROFILE:
    aws_session_params['profile_name'] = AWS_PROFILE
else:
    aws_session_params['aws_access_key_id'] = AWS_ACCESS_KEY_ID
    aws_session_params['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY

    if AWS_SESSION_TOKEN:
        aws_session_params['aws_session_token'] = AWS_SESSION_TOKEN

aws_session = boto3.Session(**aws_session_params)
s3 = aws_session.client('s3')

s3_prefix_url = urlparse(OPENSEARCH_PARQUET_PREFIX)

paginator = s3.get_paginator('list_objects_v2')
list_iterator = paginator.paginate(Bucket=s3_prefix_url.netloc, Prefix=s3_prefix_url.path.lstrip('/'))

to_delete = []


def s3_url_to_uuid(url: str) -> str:
    id_string = url.partition('job_id=')[2].split('/')[0]

    # TODO better/more consisted job_id extract? Is it needed? Check for valid UUID?

    if id_string == '':
        id_string = 'NA'

    return id_string


for page in list_iterator:
    objects = page.get('Contents', [])
    to_delete.extend([dict(Key=k['Key']) for k in objects if s3_url_to_uuid(k['Key']) in duplicated_jobs])

batches = [to_delete[i:i+1000] for i in range(0, len(to_delete), 1000)]

for batch in batches:
    logger.info(f'Deleting {len(batch):,} objects')
    resp = s3.delete_objects(Bucket=s3_prefix_url.netloc, Delete=dict(Objects=batch))

    if len(resp['Deleted']) != len(batch):
        logger.error(f'{len(resp["Errors"]):,} objects could not be deleted')

        retries = 3

        while len(resp["Errors"]) > 0 and retries > 0:
            logger.info(f'Retrying {len(resp["Errors"])} objects')
            resp = s3.delete_objects(
                Bucket=s3_prefix_url.netloc,
                Delete=dict(Objects=[dict(Key=e['Key']) for e in resp['Errors']])
            )
