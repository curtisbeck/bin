#!/usr/bin/env python3

import argparse
import elasticsearch
import json
import os

from elasticsearch.helpers import scan
from urllib.parse import urlparse


class Program:
    def main(self):
        self._parse_args()

        subdomain_urls = {}

        query = {
            'query': {'match': {'customerId': 'howstuffworks'}}
        }
        es = elasticsearch.Elasticsearch('signals-es-access-2.test.inspcloud.com')
        loop_counter = 0
        for doc in scan(es, query=query, index='signals_read', doc_type='webpage', _source=False):
            loop_counter += 1

            url = urlparse(doc['_id'])
            if url.hostname is None:
                continue

            hostname_tokens = url.hostname.split('.')
            if len(hostname_tokens) < 3:
                continue

            sub_domain = hostname_tokens[-3]
            if sub_domain not in subdomain_urls:
                subdomain_urls[sub_domain] = []

            subdomain_urls[sub_domain].append(doc['_id'])

        for k, v in subdomain_urls.items():
            file_name = 'hsw-{}.js'.format(k)
            fqn_name = os.path.join('out', 'corpus', file_name)
            with open(fqn_name, 'w') as f:
                corpus = {'urls': v}
                json.dump(corpus, f)

    def _parse_args(self):
        parser = argparse.ArgumentParser('produces json files of subdomain urls by scanning es')
        # parser.add_argument('--foo', required=True, help='placeholder')
        self.args = parser.parse_args()

if __name__ == '__main__':
    program = Program()
    program.main()
