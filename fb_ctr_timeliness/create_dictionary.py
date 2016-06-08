#!/usr/bin/env python3

import argparse
import elasticsearch
import os

from elasticsearch.helpers import scan
from gensim import corpora
from nltk.tokenize import word_tokenize

from analyzers import factory


class Program:
    def __init__(self):
        analyzer_keys = ['alpha_numeric', 'lowercase', 'remove_stopwords', 'porterstem', 'tokenize_numbers']
        self._analyzers = [factory.get_analyzer(key) for key in analyzer_keys]

    def main(self):
        self._parse_args()

        title_dict = corpora.Dictionary()
        content_dict = corpora.Dictionary()
        title_content_dict = corpora.Dictionary()

        query = {
            'query': {'match': {'customerId': 'howstuffworks'}}
        }
        es = elasticsearch.Elasticsearch('signals-es-access-1.test.inspcloud.com')
        for doc in scan(es, query=query, index='signals_read', doc_type='webpage', _source=['title', 'content']):
            title = doc['_source'].get('title', None)
            content = doc['_source'].get('content', None)

            if title is not None:
                title_tokens = self.get_analyzed_tokens(doc['_source']['title'])
                title_dict.add_documents(documents=[title_tokens])
                title_content_dict.add_documents(documents=[title_tokens])

            if content is not None:
                content_tokens = self.get_analyzed_tokens(doc['_source']['content'])
                content_dict.add_documents(documents=[content_tokens])
                title_content_dict.add_documents(documents=[content_tokens])

        title_dict.save(os.path.join('out', 'title-dict-unabridged.mm'))
        content_dict.save(os.path.join('out', 'content-dict-unabridged.mm'))
        title_content_dict.save(os.path.join('out', 'title-content-dict-unabridged.mm'))

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
