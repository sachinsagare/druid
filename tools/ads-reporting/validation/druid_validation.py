"""
Script for druid prod x stagging validation
"""

import json
import logging
import re
import requests
import sys
import os

from argparse import ArgumentParser
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)


class DruidValidation:

    _path = os.path.dirname(__file__)
    _queries_path = os.path.join(_path,  'queries/')
    _common_path = os.path.join(_path,  'common/')
    _druid_staging = 'ads-reporting-staging-query-druid.pinadmin.com'
    _druid_valiation = 'ads-reporting-staging-query-validation-druid.pinadmin.com'
    _errors = 0
    _filters = {"ads_lifetime_metrics_staging": {},
                "ads_metrics_daily_staging": {}}

    def run(self):
        self.setup()
        for filename in sorted(os.listdir(self._queries_path)):
            with open(self._queries_path+filename, encoding='utf-8', mode='r') as currentFile:
                query = re.sub(
                    "\s\s+", " ", currentFile.read().replace('\n', ''))
                query = self.setIntervals(query)
                query = self.setFilter(query)
                logging.info(f'Started query validation: {filename}')
                logging.info(f'Query: {query}')
                try:
                    result_validation = self.queryDruid(
                        self._druid_valiation, self._validation_host, query)
                    result_staging = self.queryDruid(
                        self._druid_staging, self._host, query)
                except Exception as e:
                    self._errors += 1
                    logging.error(e)
                    break
                if result_validation != result_staging:
                    self._errors += 1
                    logging.error(
                        'Query returned different values')
                    logging.error(f'Validation result: {result_validation}')
                    logging.error(f'Stagging result: {result_staging}')
                else:
                    logging.info(f'Query successfully validated: {filename}')
                    logging.info(f'Result: {result_validation}')
        if self._errors > 0:
            return 1

    def queryDruid(self, druid_instance, druid_host, query):
        headers = {'host': druid_instance,
                   'content-type': 'application/json'}
        query_endpoint = f'http://{druid_host}/druid/v2'
        logging.info(f'Querying: {query_endpoint}')
        response = requests.post(query_endpoint, data=query, headers=headers)
        if response.status_code != 200:
            raise Exception(
                f'Error on instance: {druid_instance}.\nStatus_code: {response.status_code}.\nResponse: {response.text}')
        else:
            return response.json()

    def setIntervals(self, query):
        query = json.loads(query)
        date_begin = (datetime.today() - timedelta(days=1)
                      ).strftime('%Y-%m-%d')
        date_end = datetime.today().strftime('%Y-%m-%d')
        query['intervals']['intervals'] = [
            f'{date_begin}T00:00:00.000Z/{date_end}T00:00:00.000Z']
        return json.dumps(query)

    def setFilter(self, query):
        query = json.loads(query)
        query['filter'] = {"type": "and", "fields": []}
        datasource_filter = 'ads_lifetime_metrics_staging' if 'lifetime_metrics' in query['dataSource'].get(
            'name', '') else 'ads_metrics_daily_staging'
        if 'insertion_candidate_gadvertiserid' in self._filters[datasource_filter]:
            query['filter']['fields'].append(
                {"type": "in", "dimension": "insertion_candidate_gadvertiserid_s",
                 "values": self._filters[datasource_filter]['insertion_candidate_gadvertiserid']})
        if 'insertion_candidate_gcampaignid' in self._filters[datasource_filter]:
            query['filter']['fields'].append(
                {"type": "in", "dimension": "insertion_candidate_gcampaignid_s",
                 "values": self._filters[datasource_filter]['insertion_candidate_gcampaignid']})
        return json.dumps(query)

    def setDatasource(self, query, datasource):
        query = json.loads(query)
        query['dataSource']['name'] = datasource
        return json.dumps(query)

    def setup(self):
        self.readArgs()
        self.getTopAdvertiserIDs()
        self.getTopCampaignIDs()

    def getTopAdvertiserIDs(self):
        for datasource in self._filters:
            insertion_candidate_gadvertiserid = []
            with open(self._common_path+'insertion_candidate_gadvertiserid.json', encoding='utf-8', mode='r') as currentFile:
                query = re.sub(
                    "\s\s+", " ", currentFile.read().replace('\n', ''))
                query = self.setDatasource(query, datasource)
                query = self.setIntervals(query)
                logging.info(
                    f'Getting insertion_candidate_gadvertiserid filters for {datasource}')
                logging.info(f'Query: {query}')
                try:
                    response = self.queryDruid(
                        self._druid_valiation, self._validation_host, query)
                    for result in response[0]['result']:
                        insertion_candidate_gadvertiserid.append(
                            str(result['insertion_candidate_gadvertiserid_s']))
                except Exception as e:
                    logging.error(e)
                    sys.exit(1)
            self._filters[datasource]['insertion_candidate_gadvertiserid'] = insertion_candidate_gadvertiserid
            logging.info(
                f'Using insertion_candidate_gadvertiserid filters for {datasource}: {self._filters[datasource]["insertion_candidate_gadvertiserid"]}')

    def getTopCampaignIDs(self):
        for datasource in self._filters:
            insertion_candidate_gcampaignid = []
            with open(self._common_path+'insertion_candidate_gcampaignid.json', encoding='utf-8', mode='r') as currentFile:
                query = re.sub(
                    "\s\s+", " ", currentFile.read().replace('\n', ''))
                query = self.setDatasource(query, datasource)
                query = self.setIntervals(query)
                query = self.setFilter(query)
                logging.info(
                    f'Getting insertion_candidate_gcampaignid filters for {datasource}')
                logging.info(f'Query: {query}')
                try:
                    response = self.queryDruid(
                        self._druid_valiation, self._validation_host, query)
                    for result in response[0]['result']:
                        insertion_candidate_gcampaignid.append(
                            str(result['insertion_candidate_gcampaignid']))
                except Exception as e:
                    logging.error(e)
                    sys.exit(1)
            self._filters[datasource]['insertion_candidate_gcampaignid'] = insertion_candidate_gcampaignid
            logging.info(
                f'Using insertion_candidate_gcampaignid filters for {datasource}: {self._filters[datasource]["insertion_candidate_gcampaignid"]}')

    def readArgs(self):
        parser = ArgumentParser()
        parser.add_argument('--host', default='localhost:19193')
        parser.add_argument('--validation-host', default='localhost:19193')
        args = parser.parse_args()
        self._host = args.host
        self._validation_host = args.validation_host


if __name__ == "__main__":
    druid_validation = DruidValidation()
    druid_validation.run()
