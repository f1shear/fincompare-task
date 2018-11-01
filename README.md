

#  Task

- Read csv and push records to queue
- Consume the message from queue and save it to a database table


# Requirements

- rabbitmq


# Installation

    $ pip install - r requirements.txt

    $ export QUEUE_URL='amqp://guest:guest@localhost/%2f'


# Usage

## Start worker

    $ python worker.py

## Start process_csv

    $ python process_csv --file /path/to/file.csv
