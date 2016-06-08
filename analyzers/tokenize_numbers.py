import re


class TokenizeNumbers:

    def analyze(self, tokens):
        return ['{__NUMBER__}'
                if re.match('[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?', token) is not None
                else token
                for token in tokens]
