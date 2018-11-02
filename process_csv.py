""" Process CSV file, extract valid records and push the records to queue """

import argparse
import csv
import datetime
import logging
import os
import re

from lib import Queue

logging.basicConfig(level=logging.INFO)


PUSH_BATCH_SIZE = os.environ.get('PUSH_BATCH_SIZE', 100)
QUEUE_URL = os.environ.get('QUEUE_URL', 'amqp://guest:guest@localhost/%2f')
DATA_QUEUE = 'records'


class ValidationError(ValueError):
    """ Exception class for invalid values """
    pass


class ParseError(Exception):
    """ Exception class for invalid data """
    pass


class Validator(object):
    """ Validator class with methods for data validation """

    @staticmethod
    def validate_email(value):
        """ validate email based on regex """
        pattern = r'^[\w-]+@([\w-]+\.)+[\w-]+$'
        if re.match(pattern, value):
            return value
        raise ValidationError("Invalid email format")


class RecordParser(object):
    """ RecordParser class to parse records """

    def __init__(self, validator_cls):
        self.validator_cls = validator_cls

    def parse(self, row):
        """ parse record """
        if len(row) > 1:
            name, email = row[0], row[1]
            self.validator_cls.validate_email(email)
            return {
                'name': name,
                'email': email
            }
        raise ParseError("Error parsing row: %s" % row)


class ProcessCSV(object):
    """ ProcessCSV class to process csv files push records to queue """

    @staticmethod
    def process(csv_file, parser, batch_size, queue):
        """ Read csv file, parse rows and push records to queue """
        with open(csv_file, 'r') as csvfile:
            data_reader = csv.reader(csvfile)
            batch_count = 0
            batch = []
            for row in data_reader:
                batch_count += 1
                try:
                    record = parser.parse(row)
                except ValidationError as err:
                    logging.debug(
                        '[%s] %s', datetime.datetime.utcnow(), str(err))
                except ParseError as err:
                    logging.debug(
                        '[%s] %s', datetime.datetime.utcnow(), str(err))
                batch.append(record)
                if batch_count == batch_size:
                    queue.push_records(batch)
                    batch[:] = []

            if len(batch) > 0:
                queue.push_records(batch)


def main():
    """ main function """
    arg_parser = argparse.ArgumentParser(
        description='Process file and publish data to queue.')
    arg_parser.add_argument(
        '--file', type=str, help='Path to csv file', required=True)
    args = arg_parser.parse_args()
    config = {
        'queue_url': QUEUE_URL
    }
    queue = Queue(config, DATA_QUEUE)
    parser = RecordParser(Validator)
    ProcessCSV.process(args.file, parser, PUSH_BATCH_SIZE, queue)
    queue.connection.close()


if __name__ == '__main__':
    main()
