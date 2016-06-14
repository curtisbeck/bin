#!/usr/bin/env python3

import argparse
import os

from gensim import corpora
from nltk.tokenize import word_tokenize
from urllib.parse import urlparse

from analyzers import factory
from corpus import es_customer_webpages


class Program:
    def __init__(self):
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]

    def main(self):
        self._parse_args()

        dictionaries = {}

        customer_webpages = es_customer_webpages.CustomerWebpages(self.args.customer_id)
        loop_counter = 0
        for doc in customer_webpages.corpus():
            loop_counter += 1

            sub_domain = 'all'
            if self.args.parse_subdomain:
                url = urlparse(doc['_id'])
                if url.hostname is None:
                    continue

                hostname_tokens = url.hostname.split('.')
                if len(hostname_tokens) < 3:
                    continue

                sub_domain = hostname_tokens[-3]

            if sub_domain not in dictionaries:
                dictionaries[sub_domain] = corpora.Dictionary()

            fields = ['title', 'content']
            for field in fields:
                field_value = doc['_source'].get(field, None)
                if field_value is not None:
                    tokens = self.get_analyzed_tokens(field_value)
                    dictionaries[sub_domain].add_documents(documents=[tokens])

        for k, v in dictionaries.items():
            dictionary_name = '{}-{}-unabridged.mm'.format(self.args.customer_id, k)
            v.save(os.path.join('out', 'dictionary', dictionary_name))

    def get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _parse_args(self):
        parser = argparse.ArgumentParser("creates an unabridged dictionary from a customer's webpage corpus")
        parser.add_argument('--customer_id', required=True, help='placeholder')
        parser.add_argument('--parse_subdomain', type=bool, required=False, default=False)
        self.args = parser.parse_args()

if __name__ == '__main__':
    program = Program()
    program.main()
