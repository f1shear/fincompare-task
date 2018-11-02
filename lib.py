""" Common Library """

import json

import pika


class Queue(object):
    """ Wrapper class for queue """

    def __init__(self, config, queue):
        self.connection = pika.BlockingConnection(
            pika.URLParameters(config['queue_url']))
        self.config = config
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue)
        self.queue = queue

    def publish(self, record):
        """ publish record to queue """
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue,
            body=json.dumps(record))

    def push_records(self, records):
        """ push records to queue """
        for record in records:
            self.publish(record)

    def consume(self, callback, params):
        """ consume messages from queue """
        def _callback(channel, method, properties, body):
            record = json.loads(body)
            callback(record, params)
            channel.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=self.config['pull_batch_size'])
        self.channel.basic_consume(_callback, queue=self.queue)
        self.channel.start_consuming()
