import csv
import io
import json
import logging
import os

import boto3
import dateutil.parser

import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

logger.info('Loading function')


def from_json_to_csv(data):
    writer_stream = io.StringIO()
    headers = data[0].keys()
    writer = csv.DictWriter(writer_stream, headers, dialect=utils.AWSDialect)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    writer_stream.seek(0)
    return writer_stream


def get_source_data(s3_bucket, s3_key):
    source_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    source_data = json.loads(source_obj['Body'].read())
    logger.debug(source_data)
    return source_data


def transform_source_data(report_date, source_data):
    base_currency = source_data.get('base')

    transformed_data = [{'report_date': report_date,
                         'base_currency': base_currency,
                         'currency': currency,
                         'rate': rate,
                         }
                        for currency, rate in source_data['rates'].items()]
    logger.info(f'Transformed rows: {len(source_data)}')
    return transformed_data


def main(event, context):
    destination_bucket = os.environ['DESTINATION_S3_BUCKET']
    report_date = dateutil.parser.parse(event['report_date']).strftime('%Y-%m-%d')
    source_s3_bucket = event['extractor_result']['s3_bucket']
    source_s3_key = event['extractor_result']['s3_key']
    source_data = get_source_data(source_s3_bucket, source_s3_key)

    transformed_data = transform_source_data(report_date, source_data)
    stream = from_json_to_csv(transformed_data)
    destination_s3_key = utils.get_s3_key(report_date, 'exchange_rates', 'csv')
    response = s3.put_object(Body=stream.read().encode('utf-8'),
                             Bucket=destination_bucket,
                             Key=destination_s3_key)
    logger.debug(response)
    return {'s3_key': destination_s3_key,
            's3_bucket': destination_bucket,
            's3_path': f's3://{destination_bucket}/{destination_s3_key}',
            'columns': list(transformed_data[0].keys())}


def run_lambda():
    os.environ['SLS_STAGE'] = 'dev'
    os.environ['SLS_SERVICE'] = 'etl-nico'
    os.environ['SECRET_NAME'] = 'sample_etl'
    os.environ['DESTINATION_S3_BUCKET'] = 'nicor-staging'
    test_event = {'report_date': '2018-09-12',
                  'extractor_result':
                      {'s3_bucket': 'nicor-staging',
                       's3_key': 'stage=dev/service=etl-nico/service_type=extractor/report_date=2018-09-12/exchange_rate.json'}
                  }

    r = main(test_event, None)
    print(r)
