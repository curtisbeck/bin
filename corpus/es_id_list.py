import elasticsearch
from elasticsearch.helpers import scan


class EsIdList:
    def __init__(self, id_list, fields=None, index=None, doc_type='webpage'):
        self._id_list = id_list
        self._doc_type = doc_type

        self._fields = fields
        if fields is None and doc_type == 'webpage':
            self._fields = ['title', 'content']
        elif fields is None and doc_type == 'trendpage':
            self._fields = ['description', 'name']
        elif fields is None and doc_type == 'facebookSocialMetrics':
            self._fields = ['description', 'name']

        self._index = index
        if index is None and doc_type == 'webpage':
            self._index = 'signals_read'
        elif index is None and doc_type == 'trendpage':
            self._index = 'signals_time_series_*'
        elif index is None and doc_type == 'facebookSocialMetrics':
            self._index = 'signals_time_series_20160601'

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
        loop_counter = 0
        for doc_id in self._id_list:
            loop_counter += 1

            yield self._es.get(
                index=self._index,
                id=doc_id,
                doc_type=self._doc_type,
                _source=self._fields
            )
