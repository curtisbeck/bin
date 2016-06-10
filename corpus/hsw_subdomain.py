import elasticsearch
import json
import os


class HswSubdomain:
    def __init__(self, subdomain):
        self._subdomain = subdomain
        self._es = elasticsearch.Elasticsearch(
            ['signals-es-access-1.test.inspcloud.com', 'signals-es-access-2.test.inspcloud.com'],
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            sniff_timeout=10,
            sniffer_timeout=600,
            timeout=60,
            retry_on_timeout=True,
            max_retries=10
        )

    def corpus(self):
        file_name = 'hsw-{}.js'.format(self._subdomain)
        fqn_name = os.path.join('out', 'corpus', file_name)
        with open(fqn_name) as f:
            corpus = json.load(f)

        loop_counter = 0
        for url in corpus['urls']:
            loop_counter += 1

            yield self._es.get(
                index='signals_read',
                id=url,
                doc_type='webpage',
                _source=['title', 'content']
            )
