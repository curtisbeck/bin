#!/usr/bin/env python3

import argparse
import elasticsearch
import os

from elasticsearch.helpers import scan
from gensim import corpora
from nltk.tokenize import word_tokenize
from urllib.parse import urlparse

from analyzers import factory


class Program:
    def __init__(self):
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]

    def main(self):
        self._parse_args()

        dictionaries = {}

        query = {
            'query': {'match': {'customerId': 'howstuffworks'}}
        }
        es = elasticsearch.Elasticsearch('signals-es-access-1.test.inspcloud.com')
        loop_counter = 0
        for doc in scan(es, query=query, index='signals_read', doc_type='webpage', _source=['title', 'content']):
            loop_counter += 1

            url = urlparse(doc['_id'])
            if url.hostname is None:
                continue

            hostname_tokens = url.hostname.split('.')
            if len(hostname_tokens) < 3:
                continue

            sub_domain = hostname_tokens[-3]
            if sub_domain not in dictionaries:
                dictionaries[sub_domain] = corpora.Dictionary()

            title = doc['_source'].get('title', None)
            content = doc['_source'].get('content', None)

            if title is not None:
                title_tokens = self.get_analyzed_tokens(doc['_source']['title'])
                dictionaries[sub_domain].add_documents(documents=[title_tokens])

            if content is not None:
                content_tokens = self.get_analyzed_tokens(doc['_source']['content'])
                dictionaries[sub_domain].add_documents(documents=[content_tokens])

        for k, v in dictionaries.items():
            dictionary_name = 'hsw-{}-unabridged.mm'.format(k)
            v.save(os.path.join('out', 'dictionary', dictionary_name))

    def get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _parse_args(self):
        parser = argparse.ArgumentParser('some useful description')
        # parser.add_argument('--foo', required=True, help='placeholder')
        self.args = parser.parse_args()

if __name__ == '__main__':
    program = Program()
    program.main()
