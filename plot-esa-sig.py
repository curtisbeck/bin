#!/usr/bin/env python3

import argparse
import matplotlib.pyplot as plt

from elasticsearch import Elasticsearch


class Program:
    def __init__(self):
        self._es = Elasticsearch('http://signals-es-access-1.test.inspcloud.com/')

    def main(self):
        self._parse_args()

        if self.args.sig is not None:
            self._plot_sig_arg()
        else:
            self._plot_sig_comparison()

    def _plot_sig_comparison(self):
        page_one = {}
        page_two = {}
        if self.args.webpage is not None and self.args.trendpage is None:
            page_one['url'] = self.args.webpage[0]
            page_one['esa_sig'] = EsaSig(self._get_esa_sig('signals_read', 'webpage', self.args.webpage[0]))
            page_two['url'] = self.args.webpage[1]
            page_two['esa_sig'] = EsaSig(self._get_esa_sig('signals_read', 'webpage', self.args.webpage[1]))
        elif self.args.webpage is None and self.args.trendpage is not None:
            page_one['url'] = self.args.trendpage[0]
            page_one['esa_sig'] = EsaSig(self._get_esa_sig('signals_read', 'trendpage', self.args.trendpage[0]))
            page_two['url'] = self.args.trendpage[1]
            page_two['esa_sig'] = EsaSig(self._get_esa_sig('signals_read', 'trendpage', self.args.trendpage[1]))
        else:
            page_one['url'] = self.args.webpage[0]
            page_one['esa_sig'] = EsaSig(self._get_esa_sig('signals_read', 'webpage', self.args.webpage[0]))
            page_two['url'] = self.args.trendpage[0]
            page_two['esa_sig'] = EsaSig(self._get_esa_sig('signals_read', 'trendpage', self.args.trendpage[0]))

        common_features = page_one['esa_sig'].get_common_features(page_two['esa_sig'])

        rgba_colors_one = [(1, 0, 0, 1) if name in common_features else (0, 1, 0, 0.3)
                           for name in page_one['esa_sig'].get_feature_names()]

        marker_size_one = [40 if name in common_features else 5
                           for name in page_one['esa_sig'].get_feature_names()]

        rgba_colors_two = [(1, 0, 0, 1) if name in common_features else (0, 0, 1, 0.3)
                           for name in page_two['esa_sig'].get_feature_names()]

        marker_size_two = [40 if name in common_features else 5
                           for name in page_two['esa_sig'].get_feature_names()]

        plt.scatter(
            range(len(page_one['esa_sig'].get_weights())),
            page_one['esa_sig'].get_weights(),
            marker='o',
            label=page_one['url'],
            s=marker_size_one,
            c=rgba_colors_one)

        plt.scatter(
            range(len(page_two['esa_sig'].get_weights())),
            page_two['esa_sig'].get_weights(),
            marker='s',
            label=page_two['url'],
            s=marker_size_two,
            c=rgba_colors_two)

        plt.xlabel('signature position')
        plt.ylabel('weights')
        plt.legend()
        plt.show()

    def _get_esa_sig(self, index, doc_type, id):
        resp = self._es.get(
            index=index,
            doc_type=doc_type,
            id=id,
            _source_include=['esaSignature'])

        return [float(x) for x in resp['_source']['esaSignature'].split('\t')]

    def _plot_sig_arg(self):
        esa_sig = EsaSig(self.args.sig)

        plt.plot(esa_sig.get_weights())
        plt.xlabel('feature')
        plt.ylabel('weights')
        plt.show()

    def _parse_args(self):
        parser = argparse.ArgumentParser('plots the esa signature')
        parser.add_argument('--sig', required=False, nargs='+', type=float, help='esa signature')
        parser.add_argument('--webpage', required=False, nargs='+', help='webpage url')
        parser.add_argument('--trendpage', required=False, nargs='+', help='trendpage url')
        self.args = parser.parse_args()


class EsaSig:
    def __init__(self, tokens):
        self._feature_names = []
        self._feature_weights = []
        self._feature_set = set()
        loop_count = 0
        for token in tokens:
            if loop_count % 2 == 0:
                self._feature_names.append(token)
                self._feature_set.add(token)
            else:
                self._feature_weights.append(token)
            loop_count += 1

    def get_weights(self):
        return self._feature_weights

    def get_feature_names(self):
        return self._feature_names

    def get_feature_set(self):
        return self._feature_set

    def get_common_features(self, other_sig):
        return self._feature_set & other_sig.get_feature_set()


if __name__ == '__main__':
    program = Program()
    program.main()
