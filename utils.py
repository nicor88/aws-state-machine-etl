import base64
import csv
import json
import os

import boto3
from botocore.exceptions import ClientError


class AWSDialect(csv.Dialect):
    """
    A dialect for writing values in text files optimized for loading them
    into AWS services. It uses \t as value separator to prevent comma issues
    and a singlequote as quotechar to facilitate for fields with raw JSON.

    Use removequotes parameter for the COPY command when loading to RS.
    """
    # pylint: disable=too-few-public-methods

    delimiter = '\t'
    doublequote = True
    escapechar = None
    lineterminator = '\n'
    quotechar = "'"
    quoting = csv.QUOTE_MINIMAL
    strict = True

def get_s3_key(report_date, service_type, name, key_format='json'):
    stage = os.environ['SLS_STAGE']
    service = os.environ['SLS_SERVICE']
    formatted_report_date = report_date
    return f'stage={stage}/service={service}/service_type={service_type}/report_date={formatted_report_date}/{name}.{key_format}'


def get_secret_from_secret_manager(*, secret_name):
    secret_manager = boto3.client('secretsmanager')
    try:
        secret_value_response = secret_manager.get_secret_value(SecretId=secret_name)

    except ClientError as e:
        if e.response['Error']['Code'] in ['DecryptionFailureException', 'InternalServiceErrorException',
                                           'InvalidParameterException', 'InvalidRequestException',
                                           'ResourceNotFoundException']:
            raise e
        else:
            raise Exception('Not known error')

    if 'SecretString' in secret_value_response:
        secret = secret_value_response['SecretString']

    else:
        secret = base64.b64decode(secret_value_response['SecretBinary'])
    return json.loads(secret)
