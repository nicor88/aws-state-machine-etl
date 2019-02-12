import argparse
from datetime import timedelta
import json
import logging
import os
import time

import boto3
import dateutil.parser

sfn = boto3.client('stepfunctions')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_argparser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('--start-date', required=True, help='Starting date for the backfill')
    parser.add_argument('--end-date', required=True, help='Ending date for the backfill')
    parser.add_argument('--state-machine', required=True, help='State Machine ARN')
    return parser


def generate_state_machine_input(report_date):
    return {'service_name': 'sample-etl', 'report_date': report_date, 'dispatch_type': 'backfill'}


def generate_dates(start_date_str: str, end_date_str: str):
    start_date = dateutil.parser.parse(start_date_str)
    end_date = dateutil.parser.parse(end_date_str)
    delta = end_date - start_date

    for day in range(delta.days + 1):
        yield (start_date + timedelta(days=day)).strftime('%Y-%m-%d')


def invoke_state_machine(*, state_machine_arn: str, input_json: dict):
    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(input_json)
    )
    status = response['ResponseMetadata']['HTTPStatusCode']
    logger.info(f'{input_json} Execution status: {status}')


def backfill(start_date: str, end_date: str, state_machine_arn: str):
    dates = generate_dates(start_date, end_date)

    # run sequentially, to avoid to beat the API
    for day in dates:
        input_sm = generate_state_machine_input(day)
        invoke_state_machine(state_machine_arn=state_machine_arn, input_json=input_sm)
        # sleep 5 seconds before executing the next state machine
        time.sleep(10)


if __name__ == '__main__':
    args = create_argparser().parse_args()
    start = vars(args).get('start_date')
    end = vars(args).get('end_date')
    state_machine = vars(args).get('state_machine')
    logger.info(f'Backfill from {start} till {end}')
    backfill(start, end, state_machine)
