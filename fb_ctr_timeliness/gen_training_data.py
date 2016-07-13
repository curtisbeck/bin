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

from gensim import corpora, similarities
from nltk.tokenize import word_tokenize
from scipy.sparse import coo_matrix

from analyzers import factory


class Program:
    def __init__(self):
        self._max_trend_pages_count = 20
        self._es = elasticsearch.Elasticsearch('signals-es-access-2.test.inspcloud.com')
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]
        self._filter_bow_ids = set()

    def main(self):
        self._parse_args()
        self._load_indexed_trend_ids()
        self._load_gensim_dicts()
        self._load_gensim_index()
        self._filter_dictionary()
        filename = os.path.join('data', 'HSW Facebook Posts With Post Filtered Dates.csv')
        with codecs.open(filename, encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            loop_counter = 0
            matched_trends = []
            for record in reader:
                loop_counter += 1
                print('PROCESSING', loop_counter, record, file=sys.stderr)

                doc = self._get_es_webpage(record)
                if doc is None:
                    continue

                # trend_id = self._get_best_trend_id(doc, record)
                try:
                    trend = self._get_best_trend_id_via_gensim_index(doc)
                except:
                    continue

                if trend is None:
                    continue

                try:
                    trend_pages = self._get_es_trend_pages_by_trend_id(trend['id'])
                except:
                    continue

                match = {
                    'webpage': doc['_id'],
                    'trend_id': trend['id'],
                    'trend_score': trend['score'],
                    'trend_urls': [hit['_id'] for hit in trend_pages['hits']['hits']]
                }
                matched_trends.append(match)
                continue

            matched_trends.sort(key=lambda x: x['trend_score'], reverse=True)

            with open(os.path.join('out', 'tmp.csv'), 'w') as f:
                for match in matched_trends:
                    f.write('{},{},{},{}\n'.format(
                        match['webpage'], match['trend_id'], match['trend_score'], ','.join(match['trend_urls'])))

    def _get_best_trend_id_via_gensim_index(self, doc):
        title = doc['_source']['title']
        content = doc['_source']['content']
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
        doc_bow = self._title_content_dict.doc2bow(txt_tokens)
        doc_bow = self._filter_bow(doc_bow)
        sims = self._gensim_index[doc_bow]
        best_trend = {
            'id': self._trend_ids[sims[0][0]],
            'score': sims[0][1]
        }
        return best_trend

    def _get_best_trend_id_via_esa_search(self, doc, csv_record):
        esa_sig = doc['_source']['esaSignature']
        sig_vector = _parse_esa_signature(esa_sig)
        esa_matched_trend_pages = self._get_es_trend_pages_by_signature(esa_sig)
        if esa_matched_trend_pages['hits']['total'] == 0:
            return None

        esa_matched_trend_urls = [hit['_id'] for hit in esa_matched_trend_pages['hits']['hits']]
        candidate_trend_ids = {}
        for trend_url in esa_matched_trend_urls:
            google_trends_time_series = self._get_es_google_trends_time_series_single(trend_url, csv_record['Posted'])
            if google_trends_time_series['hits']['total'] == 0:
                continue

            trend_id = google_trends_time_series['hits']['hits'][0]['_source']['trendId']
            if trend_id in candidate_trend_ids:
                candidate_trend_ids[trend_id] += 1
            else:
                candidate_trend_ids[trend_id] = 1

        if len(candidate_trend_ids) == 0:
            return None

        # trend_scores = self._score_trends_on_number_of_esa_matches(candidate_trend_ids)
        # trend_scores = self._score_trends_on_esa_similarity_to_trends_vector_sum(sig_vector, candidate_trend_ids)
        # trend_scores = self._score_trends_on_title_bow_similarity(doc['_source'], candidate_trend_ids)
        trend_scores = self._score_trends_on_title_content_bow_similarity(doc['_source'], candidate_trend_ids)

        best_trend_id = max(trend_scores.items(), key=operator.itemgetter(1))[0]
        google_trend_docs = self._get_es_google_trend(best_trend_id)

        aa_webpage = csv_record['URL']
        aa_score = trend_scores[best_trend_id]
        aa_eg_trends = list({hit['_source']['trendUrl'] for hit in google_trend_docs['hits']['hits']})

        return None

    def _score_trends_on_title_content_bow_similarity(self, webpage, trend_ids_dict):
        title = webpage.get('title', '')
        content = webpage.get('content', '')
        txt = ''
        if title is not None:
            txt += title
        if content is not None:
            txt += ' ' + content
        if txt == ' ':
            return {k: 0 for (k, v) in trend_ids_dict.items()}
        txt_tokens = self._get_analyzed_tokens(txt)
        if len(txt_tokens) == 0:
            return {k: 0 for (k, v) in trend_ids_dict.items()}
        txt_bow = self._title_content_dict.doc2bow(txt_tokens)
        if len(txt_bow) == 0:
            return {k: 0 for (k, v) in trend_ids_dict.items()}

        trend_scores = {}
        for (k, v) in trend_ids_dict.items():
            scoring_trend_pages = self._get_es_trend_pages_by_trend_id(k)
            trend_bow = {}
            for hit in scoring_trend_pages['hits']['hits']:
                title = hit['_source']['title']
                content = hit['_source']['content']
                txt = ''
                if title is not None:
                    txt += title
                if content is not None:
                    txt += ' ' + content
                txt_tokens = self._get_analyzed_tokens(txt)
                if len(txt_tokens) == 0:
                    continue
                tmp_bow = self._title_content_dict.doc2bow(txt_tokens)
                if len(tmp_bow) == 0:
                    continue
                for t in tmp_bow:
                    if t[0] in trend_bow:
                        trend_bow[t[0]] += t[1]
                    else:
                        trend_bow[t[0]] = t[1]

            trend_bow = [(k, v) for k, v in trend_bow.items()]
            v1 = _get_dictionary_feature_vector(txt_bow, self._title_content_dict)
            v2 = _get_dictionary_feature_vector(trend_bow, self._title_content_dict)
            trend_scores[k] = _get_cosine_similarity(v1, v2)
        return trend_scores

    def _score_trends_on_title_bow_similarity(self, webpage, trend_ids_dict):
        title = webpage.get('title')
        if title is None:
            return {k: 0 for (k, v) in trend_ids_dict.items()}
        title_tokens = self._get_analyzed_tokens(title)
        if len(title_tokens) == 0:
            return {k: 0 for (k, v) in trend_ids_dict.items()}
        title_bow = self._title_dict.doc2bow(title_tokens)
        if len(title_bow) == 0:
            return {k: 0 for (k, v) in trend_ids_dict.items()}

        trend_scores = {}
        for (k, v) in trend_ids_dict.items():
            scoring_trend_pages = self._get_es_trend_pages_by_trend_id(k)
            trend_bow = {}
            for hit in scoring_trend_pages['hits']['hits']:
                title = hit['_source'].get('title')
                if title is None:
                    continue
                title_tokens = self._get_analyzed_tokens(title)
                if len(title_tokens) == 0:
                    continue
                tmp_bow = self._title_dict.doc2bow(title_tokens)
                if len(tmp_bow) == 0:
                    continue
                for t in tmp_bow:
                    if t[0] in trend_bow:
                        trend_bow[t[0]] += t[1]
                    else:
                        trend_bow[t[0]] = t[1]

            trend_bow = [(v, k) for k, v in trend_bow.items()]
            v1 = _get_dictionary_feature_vector(title_bow, self._title_dict)
            v2 = _get_dictionary_feature_vector(trend_bow, self._title_dict)
            trend_scores[k] = _get_cosine_similarity(v1, v2)
        return trend_scores

    def _score_trends_on_esa_similarity_to_trends_vector_sum(self, webpage_esa_vector, trend_ids_dict):
        trend_scores = {}
        for (k, v) in trend_ids_dict.items():
            scoring_trend_pages = self._get_es_trend_pages_by_trend_id(k)
            if scoring_trend_pages is None:
                trend_scores[k] = 0
                continue
            trend_vector = _parse_esa_signature(scoring_trend_pages['hits']['hits'][0]['_source']['esaSignature'])
            for hit in scoring_trend_pages['hits']['hits'][1:]:
                trend_vector += _parse_esa_signature(hit['_source']['esaSignature'])
            trend_scores[k] = _get_cosine_similarity(webpage_esa_vector, trend_vector)
        return trend_scores

    def _score_trends_on_number_of_esa_matches(self, trend_ids_dict):
        return trend_ids_dict

    def _get_es_trend_pages_by_trend_id(self, trend_id):
        google_trend_docs = self._get_es_google_trend(trend_id)
        urls = list({hit['_source']['trendUrl'] for hit in google_trend_docs['hits']['hits']})
        return self._get_es_trend_pages_by_urls(urls)

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

    def _get_es_trend_pages_by_urls(self, urls):
        query = {
            'size': self._max_trend_pages_count,
            'query': {
                'terms': {'url': urls}
            }
        }

        return self._es.search(index='signals_read',
                               doc_type='trendpage',
                               body=query,
                               _source_include=['esaSignature', 'title', 'content'])

    def _get_es_trend_pages_by_signature(self, esa_sig):
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
                                _source=['esaSignature', 'title', 'content'])
        except:
            print('\t_get_es_webpage failed', file=sys.stderr)
            return None

    def _get_analyzed_tokens(self, string):
        tokens = word_tokenize(string)

        for analyzer in self._analyzers:
            tokens = analyzer.analyze(tokens)

        return tokens

    def _filter_bow(self, bow):
        return [x for x in bow if x[0] not in self._filter_bow_ids]

    def _load_indexed_trend_ids(self):
        with open(os.path.join('out', 'gensim-similarity-20160608-ordinal.js')) as f:
            trends = json.load(f)
        self._trend_ids = trends['ids']

    def _load_gensim_dicts(self):
        # self._title_dict = corpora.Dictionary.load(os.path.join('out', 'title-dict-unabridged.mm'))
        # self._content_dict = corpora.Dictionary.load(os.path.join('out', 'content-dict-unabridged.mm'))
        # self._content_dict.filter_extremes()
        self._title_content_dict = corpora.Dictionary.load(
            os.path.join('out', 'dictionary', 'howstuffworks-all-1000-tokens.mm'))
        # self._title_content_dict.filter_extremes(keep_n=1000)

    def _filter_dictionary(self):
        # despite gensim doc that says dictionary.filter_tokens() doesn't change ids
        # experience demonstrates otherwise
        self._filter_bow_ids.add(self._title_content_dict.token2id['{__NUMBER__}'])

    def _load_gensim_index(self):
        self._gensim_index = similarities.Similarity.load(fname=os.path.join('out', 'gensim-similarity-20160608-index'))

    def _parse_args(self):
        parser = argparse.ArgumentParser('generate training data file for linear modeling of facebook ctr')
        # parser.add_argument('--foo', required=False, help='placeholder')
        self.args = parser.parse_args()


def _get_cosine_similarity(vector_one, vector_two):
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
    return coo_matrix((data, (row, col)), shape=(100000000, 1))


def _get_dictionary_feature_vector(feature_tuples, dictionary):
    [row, data] = zip(*feature_tuples)
    col = numpy.zeros(len(row), dtype=numpy.int32)
    return coo_matrix((data, (row, col)), shape=(len(dictionary.keys()), 1))

if __name__ == '__main__':
    program = Program()
    program.main()
