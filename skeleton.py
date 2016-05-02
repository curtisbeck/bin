import argparse


class Program:

    def main(self):
        self._parse_args()
        pass

    def _parse_args(self):
        parser = argparse.ArgumentParser('some useful description')
        parser.add_argument('--foo', required=True, help='placeholder')
        self.args = parser.parse_args()

if __name__ == '__main__':
    program = Program()
    program.main()
