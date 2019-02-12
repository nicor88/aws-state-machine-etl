from contextlib import contextmanager
import logging
import os

import psycopg2

import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)


_db_connection = None

@contextmanager
def shared_connection(redshift_connection_string):
    # pylint: disable=global-statement
    global _db_connection
    if _db_connection is None:
        _db_connection = psycopg2.connect(redshift_connection_string)
        _db_connection.autocommit = True
    yield _db_connection


def get_copy_command(table_name, columns):
    columns = ', '.join(columns)
    command = f"COPY {table_name}({columns}) FROM %s " \
              f"IAM_ROLE %s DELIMITER '\t' " \
              f"IGNOREHEADER 1 TRUNCATECOLUMNS REMOVEQUOTES"
    return command


def main(event, context):
    table_name = 'dbt_dev_nicola.exchange_rates'
    s3_path = event['transformer_result']['s3_path']
    columns = event['transformer_result']['columns']
    secret_name = os.environ['SECRET_NAME']

    redshift_iam_role = utils.get_secret_from_secret_manager(secret_name=secret_name)['redshift_iam_role']
    print(redshift_iam_role)
    redshift_connection_string = utils.get_secret_from_secret_manager(
        secret_name=secret_name)['redshift_connection_string']
    copy_command = get_copy_command(table_name, columns)
    with shared_connection(redshift_connection_string) as conn:
        with conn.cursor() as cursor:
            logger.info(f'Loading values from {s3_path} to Redshift {table_name}')

            # s3_key and iam_role are params to avoid SQL injection
            cursor.execute(copy_command, (s3_path, redshift_iam_role))
            # as Redshift is built on an old postgresql fork, it doesn't support passing back rowcount
            # values after a COPY command; the recommended way is to use the pg_last_copy_count()
            # functions, see:
            # https://forums.aws.amazon.com/thread.jspa?messageID=546274#jive-message-564114
            # the operation ID can be used to query the stl_load_errors:
            # http://docs.aws.amazon.com/redshift/latest/dg/PG_LAST_COPY_ID.html
            cursor.execute('select pg_last_copy_id(), pg_last_copy_count()')
            result = cursor.fetchone()
            logger.info(f'Loaded {result[1]} records, operation ID: {result[0]}')


def run_lambda():
    os.environ['SLS_STAGE'] = 'dev'
    os.environ['SLS_SERVICE'] = 'etl-nico'
    os.environ['SECRET_NAME'] = 'sample_etl'
    test_event = {'report_date': '2018-09-12',
                  'extractor_result':
                      {'s3_bucket': 'nicor-staging',
                       's3_key': 'stage=dev/service=etl-nico/service_type=extractor/report_date=2018-09-12/exchange_rate.json'},
                  'transformer_result': {
                      's3_key': 'stage=dev/service=etl-nico/service_type=exchange_rates/report_date=2018-09-12/csv.json',
                      's3_bucket': 'nicor-staging',
                      's3_path': 's3://nicor-staging/stage=dev/service=etl-nico/service_type=exchange_rates/report_date=2018-09-12/csv.json',
                      'columns': ['report_date', 'base_currency', 'currency', 'rate']}
                  }

    result = main(test_event, None)
