#!/usr/bin/env python3

import argparse
import json
import os

from gensim import corpora, similarities
from nltk.tokenize import word_tokenize

from analyzers import factory
from corpus import fb_social_metrics, fb_trend, es_id_list


class Program:
    def __init__(self):
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]

    def main(self):
        self._parse_args()
        self._load_dictionary()
        self._load_index()
        self._load_ordinals()
        self._filter_dictionary()

        loop_counter = 0
        matches = []
        for o in self._analyzed_corpus():
            loop_counter += 1
            sims = self._index[o['bow']]
            matches.append({
                'socialMetricId': o['id'],
                'webpage': self._ordinals[sims[0][0]],
                'score': sims[0][1]
                })
        matches.sort(key=lambda x: x['score'], reverse=True)

        if self.args.fb_ids is not None:
            exit()

        output_filename = '{}-{}-{}-{}.csv'.format(
            self.args.customer_id, self.args.subdomain, self.args.fb_page, self.args.fb_dimension)
        output_filename = os.path.join('out', 'eval', output_filename)
        with open(output_filename, 'w') as f:
            for match in matches:
                f.write('{}\t{}\t{}\n'.format(match['score'], match['socialMetricId'], match['webpage']))

    def _analyzed_corpus(self):
        provider, fields = self._get_corpus_provider_and_fields()

        loop_counter = 0
        for doc in provider.corpus():
            loop_counter += 1

            bow = self._get_bow(doc, fields)
            if bow is None:
                continue

            yield {'id': doc['_id'], 'bow': bow}

    def _get_bow(self, doc, fields):
        if doc is None or len(doc['_source']) == 0:
            return None

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

    def _get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _get_corpus_provider_and_fields(self):
        if self.args.fb_dimension == 'trendpage':
            fields = ['title', 'content']
            if self.args.fb_ids is None:
                provider = fb_trend.FacebookTrends(self.args.fb_page)
            else:
                provider = es_id_list.EsIdList(self.args.fb_ids, doc_type='trendpage')
        else:
            fields = ['description', 'name']
            if self.args.fb_ids is None:
                provider = fb_social_metrics.FacebookSocialMetrics(self.args.fb_page)
            else:
                provider = es_id_list.EsIdList(self.args.fb_ids, doc_type='facebookSocialMetrics')

        return provider, fields

    def _load_dictionary(self):
        dictionary_name = '{}.mm'.format(self._partition_key)
        fqn = os.path.join('out', 'dictionary', dictionary_name)
        self._dictionary = corpora.Dictionary.load(fqn)

    def _filter_dictionary(self):
        bow = self._dictionary.doc2bow(['{__NUMBER__}'])
        bow_ids = [x[0] for x in bow]
        self._dictionary.filter_tokens(bad_ids=bow_ids)

    def _load_index(self):
        fqn_index_name = os.path.join('out', 'index', '{}-index'.format(self._partition_key))
        self._index = similarities.Similarity.load(fqn_index_name)
        self._index.output_prefix = fqn_index_name
        self._index.check_moved()

    def _load_ordinals(self):
        fqn_ordinal_name = os.path.join('out', 'index', '{}-ordinal.js'.format(self._partition_key))
        with open(fqn_ordinal_name) as f:
            data = json.load(f)
            self._ordinals = data['ids']

    def _parse_args(self):
        parser = argparse.ArgumentParser("scan facebook corpus and match to customer's corpus pages")
        parser.add_argument('--customer_id', required=True)
        parser.add_argument('--subdomain', required=True)
        parser.add_argument('--fb_page', required=True)
        parser.add_argument('--fb_dimension', choices=['social_metric', 'trendpage'], default='trendpage')
        parser.add_argument('--dictionary_version', default='unabridged')
        parser.add_argument('--fb_ids', nargs='+')
        self.args = parser.parse_args()
        self._partition_key = '{}-{}-{}'.format(
            self.args.customer_id, self.args.subdomain, self.args.dictionary_version)


if __name__ == '__main__':
    program = Program()
    program.main()
