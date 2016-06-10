#!/usr/bin/env python3

import argparse
import elasticsearch
import json

from elasticsearch.helpers import scan


class Program:

    def main(self):
        self._parse_args()
        es = elasticsearch.Elasticsearch('signals-es-access-1.test.inspcloud.com')
        ids = {hit['_source']['trendId'] for hit in
               scan(es,
                    query={"query": {"match_all": {}}},
                    index="google-trends-aggregator-20160207-000007",
                    doc_type="googleTrendsTimeSeries",
                    _source=['trendId']
                    )}
        trends = {'ids': list(ids)}
        print(json.dumps(trends))

    def _parse_args(self):
        parser = argparse.ArgumentParser('writes to stdout the trendIds as json')
        self.args = parser.parse_args()

if __name__ == '__main__':
    program = Program()
    program.main()
