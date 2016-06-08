#!/usr/bin/env python3

import argparse
import codecs
import csv
import elasticsearch
import json
import numpy
import os

from scipy.sparse import coo_matrix


class Program:
    def __init__(self):
        self._es = elasticsearch.Elasticsearch('signals-es-access-1.test.inspcloud.com')

    def main(self):
        self._parse_args()
        filename = os.path.join('data', 'HSW Facebook Posts With Post Filtered Dates.csv')
        with codecs.open(filename, encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            examples = []
            for record in reader:
                example = {
                    'webpage': record['URL'],
                    'first_hit': {},
                    'best_hit': {
                        'similarity': 0
                    }
                }
                try:
                    doc = self._get_es_webpage(record)
                except:
                    continue
                esa_sig = doc['_source']['esaSignature']
                trendpages = self._get_es_trendpages(esa_sig)
                if 0 < trendpages['hits']['total']:
                    hit = trendpages['hits']['hits'][0]
                    first_similarity = _get_cosine_similarity(esa_sig, hit['_source']['esaSignature'])
                    example['first_hit'] = {
                        'trendpage':  hit['_id'],
                        'similarity': first_similarity,
                        'hit': 1
                    }

                    hit_counter = 1
                    best_hit = {
                        'trendpage':  hit['_id'],
                        'similarity': first_similarity,
                        'hit': 1
                    }
                    for hit in trendpages['hits']['hits'][1:]:
                        hit_counter += 1
                        similarity = _get_cosine_similarity(esa_sig, hit['_source']['esaSignature'])
                        if best_hit['similarity'] < similarity:
                            best_hit = {
                                'trendpage': hit['_id'],
                                'similarity': similarity,
                                'hit': hit_counter
                            }
                    example['best_hit'] = best_hit

                examples.append(example)

            examples.sort(key=lambda x: x['best_hit']['similarity'], reverse=True)
            print(json.dumps(examples, sort_keys=True, indent=2))

    def _get_es_trendpages(self, esa_sig):
        query = {
            'size': 20,
            'query': {
                'match': {'esaSignature': esa_sig}
            }
        }

        return self._es.search(index='signals_read',
                               doc_type='trendpage',
                               body=query,
                               _source_include=['esaSignature'])

    def _get_es_webpage(self, csv_record):
        return self._es.get(index='signals_read',
                            doc_type='webpage',
                            id=csv_record['URL'],
                            _source=['esaSignature'])

    def _parse_args(self):
        parser = argparse.ArgumentParser('compares cosine sim of first trendpage match to best of first 20 matches')
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
