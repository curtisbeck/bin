#!/usr/bin/env python3

import argparse
import codecs
import csv
import datetime
import dateutil.parser
import elasticsearch
import json
import numpy
import operator
import os
import sys

from scipy.sparse import coo_matrix


class Program:
    def __init__(self):
        self._max_trend_pages_count = 20
        self._es = elasticsearch.Elasticsearch('signals-es-access-1.test.inspcloud.com')

    def main(self):
        self._parse_args()
        filename = os.path.join('data', 'HSW Facebook Posts With Post Filtered Dates.csv')
        with codecs.open(filename, encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            loop_counter = 0
            for record in reader:
                loop_counter += 1
                print('PROCESSING', loop_counter, record, file=sys.stderr)

                doc = self._get_es_webpage(record)
                if doc is None:
                    continue

                trend_id = self._get_best_trend_id(doc, record)
                if trend_id is None:
                    continue

            print(json.dumps('TBD', sort_keys=True, indent=2))

    def _get_best_trend_id(self, doc, csv_record):
        esa_sig = doc['_source']['esaSignature']
        trendpages = self._get_es_trendpages_by_signature(esa_sig)
        if trendpages['hits']['total'] == 0:
            return None

        trend_urls = [hit['_id'] for hit in trendpages['hits']['hits']]
        trend_ids = {}
        for trend_url in trend_urls:
            google_trends_time_series = self._get_es_google_trends_time_series_single(trend_url, csv_record['Posted'])
            if google_trends_time_series['hits']['total'] == 0:
                continue

            trend_id = google_trends_time_series['hits']['hits'][0]['_source']['trendId']
            if trend_id in trend_ids:
                trend_ids[trend_id] += 1
            else:
                trend_ids[trend_id] = 1

        if len(trend_ids) == 0:
            return None

        trend_scores = {}
        for (k, v) in trend_ids.items():
            google_trend_docs = self._get_es_google_trend(k)
            urls = list({hit['_source']['trendUrl'] for hit in google_trend_docs['hits']['hits']})
            scoring_trend_pages = self._get_es_trendpages_by_urls(urls)
            scores = [_get_cosine_similarity(esa_sig, hit['_source']['esaSignature'])
                      for hit in scoring_trend_pages['hits']['hits']]
            trend_scores[k] = sum(scores) / float(len(scores))

        best_trend_id = max(trend_scores.items(), key=operator.itemgetter(1))[0]
        google_trend_docs = self._get_es_google_trend(best_trend_id)

        aa = csv_record['URL']
        ab = trend_scores[best_trend_id]
        ac = list({hit['_source']['trendUrl'] for hit in google_trend_docs['hits']['hits']})

        return None

    def _get_es_google_trend(self, trend_id):
        query = {
            'size': self._max_trend_pages_count,
            'query': {
                'match': {'trendId': trend_id}
            }
        }

        return self._es.search(index='google-trends-aggregator-20160207-000007',
                               doc_type='googleTrendsTimeSeries',
                               body=query)

    def _get_es_google_trends_time_series_single(self, url, created_by):
        window_end_date = dateutil.parser.parse(created_by)
        window_begin_date = window_end_date - datetime.timedelta(days=14)
        query = {
            'size': 1,
            'query': {
                'bool': {
                    'must': [
                        {'range': {'createdTime': {'gte': window_begin_date, 'lte': window_end_date}}},
                        {'term': {'trendUrl': url}}
                    ]
                }
            }
        }

        return self._es.search(index='google-trends-aggregator-20160207-000007',
                               doc_type='googleTrendsTimeSeries',
                               body=query)

    def _get_es_google_trends_time_series(self, urls, created_by):
        window_end_date = dateutil.parser.parse(created_by)
        window_begin_date = window_end_date - datetime.timedelta(days=14)
        query = {
            'size': self._max_trend_pages_count,
            'query': {
                'bool': {
                    'must': [
                        {'range': {'createdTime': {'gte': window_begin_date, 'lte': window_end_date}}},
                        {'terms': {'trendUrl': urls}}
                    ]
                }
            }
        }

        return self._es.search(index='google-trends-aggregator-20160207-000007',
                               doc_type='googleTrendsTimeSeries',
                               body=query)

    def _get_es_trendpages_by_urls(self, urls):
        query = {
            'size': self._max_trend_pages_count,
            'query': {
                'terms': {'url': urls}
            }
        }

        return self._es.search(index='signals_read',
                               doc_type='trendpage',
                               body=query,
                               _source_include=['esaSignature'])

    def _get_es_trendpages_by_signature(self, esa_sig):
        query = {
            'size': self._max_trend_pages_count,
            'query': {
                'match': {'esaSignature': esa_sig}
            }
        }

        return self._es.search(index='signals_read',
                               doc_type='trendpage',
                               body=query,
                               _source_include=['esaSignature'])

    def _get_es_webpage(self, csv_record):
        try:
            return self._es.get(index='signals_read',
                                doc_type='webpage',
                                id=csv_record['URL'],
                                _source=['esaSignature'])
        except:
            print('\t_get_es_webpage failed', file=sys.stderr)
            return None

    def _parse_args(self):
        parser = argparse.ArgumentParser('generate training data file for linear modeling of facebook ctr')
        # parser.add_argument('--foo', required=False, help='placeholder')
        self.args = parser.parse_args()


def _get_cosine_similarity(sig_one, sig_two):
    meta_one = _parse_esa_signature(sig_one)
    meta_two = _parse_esa_signature(sig_two)
    max_row_index = max(max(meta_one['row']), max(meta_two['row']))
    shape = (max_row_index+1, 1)
    vector_one = coo_matrix((meta_one['data'], (meta_one['row'], meta_one['col'])), shape=shape)
    vector_two = coo_matrix((meta_two['data'], (meta_two['row'], meta_two['col'])), shape=shape)
    inner = vector_two.transpose().dot(vector_one)
    if inner.nnz == 0:
        return float(0)
    magnitude_one = numpy.sqrt(vector_one.transpose().dot(vector_one))
    magnitude_two = numpy.sqrt(vector_two.transpose().dot(vector_two))
    similarity = inner / (magnitude_one * magnitude_two)
    return similarity[0, 0]


def _parse_esa_signature(sig):
    sig = sig.strip()
    sig = sig.split('\t')
    row = sig[::2]
    row = [int(x) for x in row]
    col = numpy.zeros(len(row), dtype=numpy.int32)
    data = sig[1::2]
    data = [float(x) for x in data]
    return {'row': row, 'col': col, 'data': data}

if __name__ == '__main__':
    program = Program()
    program.main()
