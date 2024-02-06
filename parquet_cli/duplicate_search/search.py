

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
OPENSEARCH_STATS_INDEX = os.getenv('OPENSEARCH_STATS_INDEX')
OPENSEARCH_FILES_INDEX = os.getenv('OPENSEARCH_FILES_INDEX')
OPENSEARCH_PARQUET_PREFIX = os.getenv('OPENSEARCH_PARQUET_PREFIX')
if (OPENSEARCH_ENDPOINT is None or OPENSEARCH_PORT is None or OPENSEARCH_STATS_INDEX is None or
        OPENSEARCH_FILES_INDEX is None or OPENSEARCH_PARQUET_PREFIX is None):
    logger.error('OpenSearch parameters are not set. Please set OPENSEARCH_ENDPOINT, OPENSEARCH_PORT, '
                 'OPENSEARCH_STATS_INDEX, OPENSEARCH_FILES_INDEX, OPENSEARCH_PARQUET_PREFIX.')
    exit(1)

try:
    MAX_ES_WORKERS = int(os.getenv('MAX_ES_WORKERS'))
    assert MAX_ES_WORKERS > 0
except:
    MAX_ES_WORKERS = None

DOMS_QUERY_URL = os.getenv(
    'DOMS_QUERY_URL',
    'https://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination'
)

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

ingest_uuids = set()

scroll = helpers.scan(
    client=opensearch_client,
    scroll='2m',
    index=OPENSEARCH_STATS_INDEX
)


def s3_url_to_uuid(url: str) -> str:
    id_string = url.partition('job_id=')[2].split('/')[0]

    # TODO better/more consisted job_id extract? Is it needed? Check for valid UUID?

    return id_string


docs, added, skipped = 0, 0, 0

logger.info('Getting ingest job IDs from stats index. This may take a while to complete')

uuid_to_stats = {}  # Mapping of each ingest job to a single parquet stats entry in that job. Used to build test queries


for doc in scroll:
    docs += 1

    s3_url: str = doc['_source']['s3_url']

    if not s3_url.startswith(OPENSEARCH_PARQUET_PREFIX):
        skipped += 1
        continue

    uuid = s3_url_to_uuid(s3_url)

    added += 1
    ingest_uuids.add(uuid)
    if uuid not in uuid_to_stats or random.random() < 0.25:  # randomize the sample docs
        uuid_to_stats[uuid] = doc['_source']

stats_list_time = datetime.now()

logger.info(f'Found {len(ingest_uuids):,} unique ingest job IDs from {added:,} ES records (out of {docs:,} | '
            f'skipped: {skipped:,}). Took {stats_list_time - start_time}')

ingest_uuids = list(ingest_uuids)

unconfirmed_duplicates = []
confirmed_duplicates = []

src_id_map = dict()


def get_es_docs_for_id(id: str):
    records = []

    retries = 3
    retry_delay = 1.5

    while retries > 0:
        try:
            records = []

            scroll = helpers.scan(
                client=opensearch_client,
                scroll='2m',
                index=OPENSEARCH_FILES_INDEX,
                query={"query": {"match": {"uuid": id}}}
            )

            for doc in scroll:
                records.append(doc)

            break
        except Exception as e:
            logger.error(f'An exception occurred: {e}')
            retries -= 1
            sleep(retry_delay)
            retry_delay *= 2

    return id, records


logger.info('Fetching ingest file records for each ingest job')

ingest_records = []

with ThreadPoolExecutor(max_workers=MAX_ES_WORKERS) as pool:
    futures = []

    for ingest_id in ingest_uuids:
        f = pool.submit(get_es_docs_for_id, ingest_id)
        futures.append(f)

    for i, result in enumerate(as_completed(futures), start=1):
        ingest_records.append(result.result())

        if i % 250 == 0:
            logger.info(f'Fetched {i:,} ingestion records')

record_pull_time = datetime.now()
logger.info(f'Fetches complete. Took {record_pull_time - stats_list_time}')
logger.info('Mapping ingest source files to job IDs')

for ingest_id, es_records in ingest_records:
    if len(es_records) == 0:
        unconfirmed_duplicates.append(ingest_id)
    else:
        for doc in es_records:
            src_url = doc['_source']['s3_url']
            time = doc['_source']['job_end_time']

            if src_url not in src_id_map:
                src_id_map[src_url] = [(ingest_id, time)]
            else:
                src_id_map[src_url].append((ingest_id, time))

for duplicates in [src_id_map[src] for src in src_id_map if len(src_id_map[src]) > 1]:
    duplicate_jobs = list(sorted(duplicates, key=lambda x: x[1], reverse=True))[1:]
    confirmed_duplicates.extend([d[0] for d in duplicate_jobs])

map_complete_time = datetime.now()
logger.info(f'Mapping complete in {map_complete_time - record_pull_time}')

if len(unconfirmed_duplicates) == 0 and len(confirmed_duplicates) == 0:
    logger.info('No duplicate or suspected duplicate ingests found')
else:
    suspected_duplicates = []
    missing_ingest_records = []

    for uuid in unconfirmed_duplicates:
        logger.info(f'Checking ingest job {uuid}')

        try:
            parquet_stats = uuid_to_stats[uuid]

            min_lon = parquet_stats['min_lon']
            min_lat = parquet_stats['min_lat']
            max_lon = parquet_stats['max_lon']
            max_lat = parquet_stats['max_lat']

            params = dict(
                provider=parquet_stats['provider'],
                project=parquet_stats['project'],
                platform=parquet_stats['platform_code'],
                bbox=f'{min_lon},{min_lat},{max_lon},{max_lat}',
                minDepth=parquet_stats['min_depth'],
                maxDepth=parquet_stats['max_depth'],
                startTime=datetime.utcfromtimestamp(parquet_stats['min_datetime']).strftime('%Y-%m-%dT%H:%M:%S%zZ'),
                endTime=datetime.utcfromtimestamp(parquet_stats['max_datetime']).strftime('%Y-%m-%dT%H:%M:%S%zZ'),
                itemsPerPage=20000
            )

            session = requests.session()
            next_url = DOMS_QUERY_URL

            insitu_points = []

            while next_url is not None and next_url != 'NA':
                delay = 1
                retries = 2

                response = session.get(next_url, params=params)

                while retries > 0 and response.status_code != 200:
                    sleep(delay)
                    delay *= 2
                    response = session.get(next_url, params=params)

                response.raise_for_status()
                insitu_response = response.json()

                insitu_points.extend(insitu_response['results'])

                next_url = insitu_response.get('next', None)
                params = {}

            insitu_dict = {}

            for p in insitu_points:
                key = (p['time'], p['latitude'], p['longitude'], p['depth'])
                if key not in insitu_dict:
                    insitu_dict[key] = [p['job_id']]
                else:
                    insitu_dict[key].append(p['job_id'])

            if any([len(insitu_dict[k]) > 1 for k in insitu_dict]):
                logger.info(f'Ingest job {uuid} has duplicates')
                confirmed_duplicates.append(uuid)
            else:
                missing_ingest_records.append(uuid)
        except:
            suspected_duplicates.append(uuid)

    logger.info(f'Found {len(confirmed_duplicates):,} confirmed duplicate ingests and {len(suspected_duplicates):,} '
                f'suspected duplicate ingests. Writing to search_result.json')

    output = dict(
        confirmed_duplicates=confirmed_duplicates,
        suspected_duplicates=suspected_duplicates,
        missing_ingest_records=missing_ingest_records
        # debug=dict(src_id_map=src_id_map)
    )

    with open('search_result.json', 'w') as fp:
        json.dump(output, fp, indent=4)

logger.info(f'Finished in {datetime.now() - start_time}')
