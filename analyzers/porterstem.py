from nltk.stem import *


class Porterstem:

    def analyze(self, tokens):
        stemmer = SnowballStemmer('english')
        return [stemmer.stem(t) for t in tokens]
