import argparse

from elasticsearch import Elasticsearch, helpers


class Program:

    def main(self):
        self._parse_args()

        es = Elasticsearch(self.args.host)

        query = {
            'fields': []
        }

        scan = helpers.scan(es,
                            index=self.args.index,
                            doc_type=self.args.doc_type,
                            query=query,
                            scroll='5m')

        field = self.args.field
        script_template = """if (ctx._source.containsKey("{}") && ctx._source["{}"] instanceof LinkedHashMap)
                                ctx._source["{}"] = [ctx._source["{}"]]
                             else
                                ctx.op = "none" """
        script = script_template.format(field, field, field, field)

        bulk_update_query = []
        processed_count = 0
        for doc in scan:
            processed_count += 1
            bulk_update_query.append(
                {
                 '_op_type': 'update',
                 '_index': self.args.index,
                 '_type': self.args.doc_type,
                 '_retry_on_conflict': 3,
                 '_id': doc['_id'],
                 'script': script
                })

            if 99 < len(bulk_update_query):
                print("processed {} docs".format(processed_count))
                helpers.bulk(es, bulk_update_query)
                bulk_update_query = []

        if 0 < len(bulk_update_query):
            helpers.bulk(es, bulk_update_query)

    def _parse_args(self):
        parser = argparse.ArgumentParser('Converts an elasticsearch field to an array of that type')
        parser.add_argument('--host', required=True, help='elasticsearch host')
        parser.add_argument('--index', required=True, help='elasticsearch index')
        parser.add_argument('--doc_type', required=True, help='elasticsearch type')
        parser.add_argument('--field', required=True, help='elasticsearch field to convert')
        self.args = parser.parse_args()


if __name__ == '__main__':
    program = Program()
    program.main()
