from cleo import Command
import csv
import os
import pika
import json

FILE_NAME = 'planet-latest_geonames_test.tsv'


class PublisherCommand(Command):
    """
     Publisher

     publisher
    """
    channel_forward = None
    channel_callback = None
    reader = None
    counter = 0

    def handle(self):

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
        # connect to rabbit to forward queue
        self.channel_forward = connection.channel()
        self.channel_forward.queue_declare(queue='forward', durable=True)

        #connect to rabbit to callback queue
        self.channel_callback = connection.channel()
        self.channel_callback.queue_declare(queue='callback', durable=True)

        # read file
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'files'))
        file_path = '{}/{}'.format(path, FILE_NAME)
        f = open(file_path, 'rt')
        try:
            self.reader = csv.reader(f, delimiter='\t')
            self.__push_to_queue(1000)

            #listen callback queue
            self.channel_callback.basic_qos(prefetch_count=1)
            self.channel_callback.basic_consume(self.__callback,
                                                queue='callback')

            self.channel_callback.start_consuming()
        finally:
            f.close()

    def __push_to_queue(self, n):
        try:
            for i in range(n+1):
                row = self.reader.__next__()
                if row[0] == 'name':
                    continue
                self.__publish(json.dumps(row))
        except StopIteration:
            self.__publish('end')

    def __callback(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if body == 'end':
            exit(1)

        count = int(body)
        self.counter += count
        self.write("\r{}".format(self.counter))
        self.__push_to_queue(count)

    def __publish(self, message):
        self.channel_forward.basic_publish(exchange='',
                                           routing_key='forward',
                                           body=message,
                                           properties=pika.BasicProperties(
                                               delivery_mode=2,  # make message persistent
                                           ))
