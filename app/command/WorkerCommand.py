from cleo import Command
import pika
import json
import requests
import os
import csv
from lxml import html
import psycopg2
import re

LOOKUP_URL = 'https://osm-dev.eos.com/nominatim/lookup?osm_ids={}&format=json'
DETAILS_URL = 'https://osm-dev.eos.com/nominatim/details.php?place_id={}'

class WorkerCommand(Command):
    """
     Worker

     worker
        {--number= : number}
    """
    channel_forward = None
    channel_callback = None
    writer = None
    cursor = None

    def handle(self):
        number = int(self.option('number'))
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'files'))
        file_path_updated = '{}/{}_{}.tsv'.format(path, 'planet-latest_geonames_test_update', number)
        f_up = open(file_path_updated, 'at')
        self.writer = csv.writer(f_up, delimiter='\t')

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel_forward = connection.channel()
        self.channel_forward.queue_declare(queue='forward', durable=True)

        self.channel_callback = connection.channel()
        self.channel_callback.queue_declare(queue='callback', durable=True)

        conn = psycopg2.connect(host="127.0.0.1",
                                     database="nominatim",
                                     user="fil",
                                     password="euF5PSdLjrAzcg9d",
                                     port=2222)
        self.cursor = conn.cursor()

        def callback(ch, method, properties, body):
            if body == 'end':
                self.__publish('end')
            else:
                # print(" [x] Received %r" % json.loads(body))
                self.__process(list(json.loads(body)))
                self.__publish('1')
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel_forward.basic_qos(prefetch_count=1)
        self.channel_forward.basic_consume(callback,
                                           queue='forward')

        self.channel_forward.start_consuming()

    def __process1(self, row):
        place_id = self.__get_place_id('{}{}'.format(row[2][0].upper(), row[3]))
        if place_id is None:
            self.writer.writerow(row)
        else:
            info = self.__get_additional_info(place_id)
            row.append(info.get('population', 0))
            row.append(info.get('is_capital'))
            row[1] = ','.join(list(set(row[1].split(',') + info.get("alternative_name"))))
            self.writer.writerow(row)

    def __get_place_id(self, osm_id):
        response = requests.get(LOOKUP_URL.format(osm_id))
        data = json.loads(response.text)
        if len(data) == 0:
            return None
        return int(data[0].get('place_id'))

    def __get_additional_info(self, place_id):
        response = requests.get(DETAILS_URL.format(place_id))
        tree = html.fromstring(response.text)
        alternative_names = tree.xpath('//table[@id="locationdetails"]/tr[1]/td[2]/div/span/text()')
        population_value = tree.xpath('//table[@id="locationdetails"]/tbody/tr[13]/td[2]/div[3]/span/text()')
        population = int(population_value[0]) if len(population_value) != 0 else 0
        capital_value = tree.xpath('//table[@id="locationdetails"]/tbody/tr[13]/td[2]/div[1]/text()')
        is_capital = True if len(capital_value) != 0 else False
        return {"alternative_name": alternative_names, "population": population, "is_capital": is_capital}

    def __publish(self, message):
        self.channel_callback.basic_publish(exchange='',
                                            routing_key='callback',
                                            body=message,
                                            properties=pika.BasicProperties(
                                                delivery_mode=2,  # make message persistent
                                            ))

    def __process(self, row):
        osm_type = row[2][0].upper()
        osm_id = row[3]
        self.cursor.execute("""
SELECT
    place_id,
    osm_type,
    osm_id,
    name,
    importance,
    parent_place_id,
    rank_address,
    rank_search,
    CASE
       WHEN importance = 0 OR importance IS NULL
       THEN 0.75-(rank_search::float/40)
       ELSE importance
       END as calculated_importance,
    extratags
FROM placex
WHERE osm_type = %s AND osm_id = %s
""", (osm_type, osm_id))

        result = self.cursor.fetchall()

        if len(result) > 0:
            name_list = re.findall('=>"(.*?)"', result[0][3])

            if result[0][9] is not None:
                population_list = re.findall('population"=>"(.*?)"', result[0][9])
                population = int(population_list[0].replace(',', '').replace('<', '')) if len(population_list) > 0 else 0
                capital = bool(re.match('capital"="yes"', result[0][9]))
            else:
                population = 0
                capital = False

            row.append(population)
            row.append(capital)
            row[1] = ','.join(list(set(row[1].split(',') + name_list)))

        self.writer.writerow(row)

        # place_id = self.__get_place_id('{}{}'.format(row[2][0].upper(), row[3]))
        # if place_id is None:
        #     self.writer.writerow(row)
        # else:
        #     info = self.__get_additional_info(place_id)
        #     row.append(info.get('population', 0))
        #     row.append(info.get('is_capital'))
        #     row[1] = ','.join(list(set(row[1].split(',') + info.get("alternative_name"))))
        #     self.writer.writerow(row)
