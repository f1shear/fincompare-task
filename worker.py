""" Worker """

import datetime
import logging
import os
import sqlite3

from lib import Queue

PUSH_BATCH_SIZE = os.environ.get('PUSH_BATCH_SIZE', 100)
PULL_BATCH_SIZE = os.environ.get('PULL_BATCH_SIZE', 100)
QUEUE_URL = os.environ.get('QUEUE_URL', 'amqp://guest:guest@localhost/%2f')

DATA_QUEUE = 'records'


logging.basicConfig(level=logging.INFO)


def callback(record, params):
    """Callback to process record after receiving record from queue
        Args:
            record (dict): The record containing name and email
            params (dict): Extra arguments like database cursor, connection
        Returns:
            None
    """
    cursor = params['cursor']
    conn = params['conn']
    cursor.execute(
        'SELECT count(email) FROM records WHERE email="%s"' % record['email'])
    exists = cursor.fetchone()[0] > 0
    if not exists:
        cursor.execute("""
            INSERT INTO records (name, email)
            VALUES ('%(name)s', '%(email)s');
            """ % record)
        logging.info("[%s] Added record", datetime.datetime.utcnow())
        conn.commit()
    else:
        logging.info("[%s] Record Already exists", datetime.datetime.utcnow())


def setup_db(conn):
    """Setup database, create table records if it does not exist
        Args:
            conn (obj): Sqlite3 database connection object
        Returns:
            None
    """
    cur = conn.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS records
             (name text, email text UNIQUE)''')
    conn.commit()


def main():
    """Main function"""
    config = {
        'queue_url': QUEUE_URL,
        'pull_batch_size': PULL_BATCH_SIZE,
        'db_path': ":memory:"
    }
    queue = Queue(config, DATA_QUEUE)
    conn = sqlite3.connect(":memory:")

    setup_db(conn)

    try:
        params = {
            'conn': conn,
            'cursor': conn.cursor()
        }
        queue.consume(callback, params)
    except Exception as err:
        logging.error(err)
    conn.close()
    queue.channel.close()


if __name__ == '__main__':
    main()
