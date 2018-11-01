
import argparse
import csv
import os


from lib import Queue


PUSH_BATCH_SIZE = os.environ.get('PUSH_BATCH_SIZE', 100)
QUEUE_URL = os.environ.get('QUEUE_URL', 'amqp://guest:guest@localhost/%2f')
DATA_QUEUE = 'records'


class RecordParser(object):

    @staticmethod
    def parse(row):
        return {
            'name': row[0],
            'email': row[1]
        }


class ProcessCSV(object):

    @staticmethod
    def process(file, parser_cls, batch_size, queue):
        with open(file, 'r') as csvfile:
            data_reader = csv.reader(csvfile)
            batch_count = 0
            batch = []
            for row in data_reader:
                batch_count += 1
                record = parser_cls.parse(row)
                batch.append(record)
                if batch_count == batch_size:
                    queue.push_records(batch)
                    batch[:] = []

            if len(batch) > 0:
                queue.push_records(batch)


def main():
    parser = argparse.ArgumentParser(
        description='Process file and publish data to queue.')
    parser.add_argument(
        '--file', type=str, help='Path to csv file', required=True)
    args = parser.parse_args()
    config = {
        'queue_url': QUEUE_URL
    }
    queue = Queue(config, DATA_QUEUE)
    ProcessCSV.process(args.file, RecordParser, PUSH_BATCH_SIZE, queue)
    queue.connection.close()


if __name__ == '__main__':
    main()
