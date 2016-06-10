#!/usr/bin/env python3

import argparse
import elasticsearch
import json
import os
import sys

from gensim import corpora, similarities
from nltk.tokenize import word_tokenize

from analyzers import factory
from corpus import hsw_subdomain


class Program:
    def __init__(self):
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]
        self._indexed_urls = []

    def main(self):
        self._parse_args()
        self._load_dictionary()

        index_root_path = os.path.join('out', 'index', 'hsw')

        shard_prefix = os.path.join(index_root_path, '{}-shard'.format(self._partition_key))
        index = similarities.Similarity(
            shard_prefix, self._analyzed_corpus(), num_features=max(self._dictionary.keys()), num_best=10)

        fqn_index_name = os.path.join(index_root_path, '{}-index'.format(self._partition_key))
        index.save(fname=fqn_index_name)

        ordinal_ids = {'ids': self._indexed_urls}
        fqn_ordinal_name = os.path.join(index_root_path, '{}-ordinal.js'.format(self._partition_key))
        with open(fqn_ordinal_name, 'w') as f:
            json.dump(ordinal_ids, f)

    def _analyzed_corpus(self):
        hsw = hsw_subdomain.HswSubdomain(self.args.subdomain)
        loop_counter = 0
        for doc in hsw.corpus():
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

        title = doc['_source'].get('title')
        content = doc['_source'].get('content')
        if title is None and content is None:
            return None

        txt = ''
        if title is not None:
            txt += title
        if content is not None:
            txt += ' ' + content

        txt_tokens = self._get_analyzed_tokens(txt)
        if len(txt_tokens) == 0:
            return None

        bow = self._dictionary.doc2bow(txt_tokens)
        if len(bow) == 0:
            return None

        return bow

    def _load_dictionary(self):
        dictionary_name = 'hsw-{}.mm'.format(self._partition_key)
        fqn = os.path.join('out', 'dictionary', dictionary_name)
        self._dictionary = corpora.Dictionary.load(fqn)

    def _get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _parse_args(self):
        parser = argparse.ArgumentParser('generates the gensim index')
        parser.add_argument('--subdomain', required=True)
        parser.add_argument('--dictionary_version', default='unabridged')
        self.args = parser.parse_args()
        self._partition_key = '{}-{}'.format(self.args.subdomain, self.args.dictionary_version)

if __name__ == '__main__':
    program = Program()
    program.main()
