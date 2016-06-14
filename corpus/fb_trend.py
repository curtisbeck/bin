import elasticsearch
from elasticsearch.helpers import scan


class FacebookTrends:
    def __init__(self, page_name):
        self._page_name = page_name
        self._parent_page_id = None
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

        self._parent_page_id = response['hits']['hits'][0]['_source']['nodeId']

        for trend in self.page_trend_corpus():

            try:
                hit = self._es.get(
                    index='signals_read',
                    doc_type='trendpage',
                    id=trend['_source']['url']
                )
                yield hit
            except:
                continue

    def page_trend_corpus(self):
        for hit in self.facebook_trend_corpus():
            if hit['_source']['parentPageId'] == self._parent_page_id:
                yield hit

    def facebook_trend_corpus(self):
        return scan(self._es,
                    query={"query": {"match_all": {}}},
                    index="signals_time_series_20160601",
                    doc_type="facebookTrend",
                    _source=['url', 'parentPageId']
                    )
