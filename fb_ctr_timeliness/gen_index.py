#!/usr/bin/env python3

import argparse
import json
import os
import sys

from gensim import corpora, similarities
from nltk.tokenize import word_tokenize

from analyzers import factory
from corpus import hsw_subdomain, es_customer_webpages


class Program:
    def __init__(self):
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]
        self._indexed_urls = []

    # the saved index contains the relative path of the shards. So create everything
    # in current directory and then move to final directory.
    def main(self):
        self._parse_args()
        self._load_dictionary()

        shard_prefix = '{}-shard'.format(self._partition_key)
        index = similarities.Similarity(
            shard_prefix, self._analyzed_corpus(), num_features=len(self._dictionary), num_best=10)

        fqn_index_name = '{}-index'.format(self._partition_key)
        index.save(fname=fqn_index_name)

        ordinal_ids = {'ids': self._indexed_urls}
        fqn_ordinal_name = '{}-ordinal.js'.format(self._partition_key)
        with open(fqn_ordinal_name, 'w') as f:
            json.dump(ordinal_ids, f)

        index.close_shard()
        for item in os.listdir('.'):
            if item.startswith(self._partition_key):
                os.rename(item, os.path.join('out', 'index', item))

    def _analyzed_corpus(self):
        if self.args.subdomain == 'all':
            provider = es_customer_webpages.CustomerWebpages(self.args.customer_id)
        else:
            provider = hsw_subdomain.HswSubdomain(self.args.subdomain)
        loop_counter = 0
        for doc in provider.corpus():
            loop_counter += 1
            print('{} {}'.format(loop_counter, doc['_id']), file=sys.stderr)

            bow = self._get_bow(doc)
            if bow is None:
                continue

            self._indexed_urls.append(doc['_id'])
            yield bow

    def _get_bow(self, doc):
        if doc is None or len(doc['_source']) == 0:
            return None

        fields = ['title', 'content']
        field_data = []
        for field in fields:
            value = doc['_source'].get(field)
            if value is None:
                continue
            field_data.append(value)

        txt = ' '.join(field_data)

        txt_tokens = self._get_analyzed_tokens(txt)
        if len(txt_tokens) == 0:
            return None

        bow = self._dictionary.doc2bow(txt_tokens)
        if len(bow) == 0:
            return None

        return bow

    def _load_dictionary(self):
        dictionary_name = '{}.mm'.format(self._partition_key)
        fqn = os.path.join('out', 'dictionary', dictionary_name)
        self._dictionary = corpora.Dictionary.load(fqn)

    def _get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _parse_args(self):
        parser = argparse.ArgumentParser('generates the gensim index')
        parser.add_argument('--customer_id', required=True)
        parser.add_argument('--subdomain', required=True)
        parser.add_argument('--dictionary_version', default='unabridged')
        self.args = parser.parse_args()
        self._partition_key = '{}-{}-{}'.format(self.args.customer_id, self.args.subdomain, self.args.dictionary_version)

if __name__ == '__main__':
    program = Program()
    program.main()
