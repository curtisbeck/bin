import re


class AlphaNumeric:

    def analyze(self, tokens):
        return [token for token in tokens if re.search('[a-zA-Z0-9]', token) is not None]
