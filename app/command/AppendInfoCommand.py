from cleo import Command
import csv
import json
import requests
import os
from lxml import html

LOOKUP_URL = 'https://osm-dev.eos.com/nominatim/lookup?osm_ids={}&format=json'
DETAILS_URL = 'https://osm-dev.eos.com/nominatim/details.php?place_id={}'


class AppendInfoCommand(Command):
    """
     Load email confirm data and social provider from csv (user_id, confirm, provider) and append it to user data

     append:info
         {--path= : Path to csv file. Set absolute path}
    """
    def handle(self):
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'files'))
        file_path = '{}/{}'.format(path, 'planet-latest_geonames_test.tsv')
        file_path_updated = '{}/{}'.format(path, 'planet-latest_geonames_test_update.tsv')

        f = open(file_path, 'rt')
        f_up = open(file_path_updated, 'wt')

        try:
            reader = csv.reader(f, delimiter='\t')
            writer = csv.writer(f_up, delimiter='\t')
            i = 0
            for row in reader:
                if row[0] == 'name':
                    continue
                # do request to nominatim lookup
                # read place_id
                place_id = self.__get_place_id('{}{}'.format(row[2][0].upper(), row[3]))

                # do request to nominatim details
                # parse request and append data to tsv file
                info = self.__get_additional_info(place_id)
                row.append(info.get('population', 0))
                row.append(info.get('is_capital'))
                row[1] = ','.join(list(set(row[1].split(',') + info.get("alternative_name"))))
                writer.writerow(row)

                i += 1
                print("\r{} processed".format(i))

        finally:
            f.close()
            f_up.close()

        # self.line("\nDone!")

    def __get_place_id(self, osm_id):
        response = requests.get(LOOKUP_URL.format(osm_id))
        data = json.loads(response.text)
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
