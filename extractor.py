import json
import logging
import os

import boto3
import dateutil.parser
import requests as r

import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Loading function')

BASE_URL = 'https://openexchangerates.org/api/'

s3 = boto3.client('s3')


def fetch_exchange_rates(app_id, report_date):
    # https://docs.openexchangerates.org/docs/historical-json
    params = {'app_id': app_id, 'base': 'USD'}
    url = f'{BASE_URL}/historical/{report_date}.json'
    logger.info(url)
    response = r.get(url, params=params)
    logger.info(response.status_code)
    response.raise_for_status()
    return response.json()


def main(event, context):
    logger.info(event)
    destination_s3_bucket = os.environ['DESTINATION_S3_BUCKET']
    secret_name = os.environ['SECRET_NAME']
    report_date = dateutil.parser.parse(event['report_date']).strftime('%Y-%m-%d')
    app_id = utils.get_secret_from_secret_manager(secret_name=secret_name)['exchange_rate_app_id']
    exchange_rates = fetch_exchange_rates(app_id, report_date)
    logger.debug(exchange_rates)
    to_write = json.dumps(exchange_rates)
    s3_key = utils.get_s3_key(report_date, 'extractor', 'exchange_rate', 'json')
    response = s3.put_object(Bucket=destination_s3_bucket,
                             Key=s3_key,
                             Body=to_write.encode('utf-8'))
    logger.debug(response)
    return {'s3_bucket': destination_s3_bucket, 's3_key': s3_key}


def run_lambda():
    os.environ['SLS_STAGE'] = 'dev'
    os.environ['SLS_SERVICE'] = 'etl-nico'
    os.environ['SECRET_NAME'] = 'sample_etl'
    os.environ['DESTINATION_S3_BUCKET'] = 'nicor-staging'
    test_event = {'report_date': '2018-09-12'}
    result = main(test_event, None)
    print(result)
