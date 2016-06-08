#!/usr/bin/env python3

import argparse
import elasticsearch
import json
import os
import sys

from gensim import corpora, similarities
from nltk.tokenize import word_tokenize

from analyzers import factory


class Program:
    def __init__(self):
        self._es = elasticsearch.Elasticsearch('signals-es-access-1.test.inspcloud.com')
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]

    def main(self):
        self._parse_args()
        self._load_gensim_dicts()
        index = similarities.Similarity(os.path.join('out', 'index'), [], num_features=1000, num_best=10)
        for doc_bow in self._corpus_generator():
            index.add_documents([doc_bow])
        index.save(fname=os.path.join('out', 'gensim-matrix-similarity'))

    def _corpus_generator(self):
        with open(os.path.join('out', 'trend-ids.js')) as f:
            trends = json.load(f)
        loop_counter = 0
        for trend_id in trends['ids']:
            loop_counter += 1
            print('{}/{} {}'.format(loop_counter, len(trends['ids']), trend_id), file=sys.stderr)

            trend_bow = self._get_trend_bow(trend_id)
            if trend_bow is None:
                continue

            yield trend_bow

    def _get_trend_bow(self, trend_id):
        docs = self._get_es_trend_pages_by_trend_id(trend_id)
        if docs is None:
            return None
        trend_bow = {}
        for hit in docs['hits']['hits']:
            title = hit['_source']['title']
            content = hit['_source']['content']
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
            hit_bow = self._title_content_dict.doc2bow(txt_tokens)
            if len(hit_bow) == 0:
                return None
            for t in hit_bow:
                if t[0] in trend_bow:
                    trend_bow[t[0]] += t[1]
                else:
                    trend_bow[t[0]] = t[1]

        trend_bow = [(k, v) for k, v in trend_bow.items()]
        return trend_bow

    def _get_es_trend_pages_by_trend_id(self, trend_id):
        google_trend_docs = self._get_es_google_trend(trend_id)
        urls = list({hit['_source']['trendUrl'] for hit in google_trend_docs['hits']['hits']
                     if hit['_source']['trendUrl'] is not None})
        return self._get_es_trend_pages_by_urls(urls)

    def _get_es_google_trend(self, trend_id):
        query = {
            'size': 1000,
            'query': {
                'match': {'trendId': trend_id}
            }
        }

        return self._es.search(index='google-trends-aggregator-20160207-000007',
                               doc_type='googleTrendsTimeSeries',
                               body=query)

    def _get_es_trend_pages_by_urls(self, urls):
        if urls is None or len(urls) == 0:
            return None

        query = {
            'size': 1000,
            'query': {
                'terms': {'url': urls}
            }
        }

        return self._es.search(index='signals_read',
                               doc_type='trendpage',
                               body=query,
                               _source_include=['title', 'content'])

    def _load_gensim_dicts(self):
        self._title_content_dict = corpora.Dictionary.load(os.path.join('out', 'title-content-dict-unabridged.mm'))
        self._title_content_dict.filter_extremes(keep_n=1000)

    def _get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _parse_args(self):
        parser = argparse.ArgumentParser('caches the gensim similarity index of the trend bow')
        self.args = parser.parse_args()

if __name__ == '__main__':
    program = Program()
    program.main()
