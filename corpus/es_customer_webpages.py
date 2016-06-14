import elasticsearch
from elasticsearch.helpers import scan


class CustomerWebpages:
    def __init__(self, customer_id):
        self._customer_id = customer_id
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
        return scan(self._es,
                    query={"query": {"match": {"customerId": self._customer_id}}},
                    index='signals_read',
                    doc_type='webpage',
                    _source=['title', 'content']
                    )
