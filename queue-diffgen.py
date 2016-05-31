#!/usr/bin/env python3

import argparse
import json
import fileinput

from boto3 import resource as aws


class Program:

    def main(self):
        self._parse_args()

        urls = []
        for line in fileinput.input():
            url = line.strip()
            if url is not None:
                urls.append(url)

        client = aws('sqs')
        queue = client.get_queue_by_name(QueueName='signals-diffgen-test')
        for chunk in chunks(urls, 20):
            msg = {
                'urls': chunk
            }

            msg_body = json.dumps(msg)
            queue.send_message(MessageBody=msg_body)

    def _parse_args(self):
        description = 'reads a list of urls from stdin and sends them in chunks to signals-diffgent-test queue'
        parser = argparse.ArgumentParser(description)
        # parser.add_argument('--foo', required=True, help='placeholder')
        self.args = parser.parse_args()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

if __name__ == '__main__':
    program = Program()
    program.main()
