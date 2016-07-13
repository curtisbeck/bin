import elasticsearch
from elasticsearch.helpers import scan


class FacebookSocialMetrics:
    def __init__(self, page_name):
        self._page_name = page_name
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

        response = self._es.search(
            body={"query": {"match": {"name": self._page_name}}},
            index='signals_read',
            doc_type='facebookTrendPage',
            _source=['nodeId']
        )

        node_id = response['hits']['hits'][0]['_source']['nodeId']

        return scan(self._es,
                    query={"query": {"match": {"parentPageId": node_id}}},
                    index="signals_time_series_20160601",
                    doc_type="facebookSocialMetrics",
                    _source=['description', 'name', 'message']
                    )
