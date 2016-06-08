from nltk.corpus import stopwords


class RemoveStopwords:

    def analyze(self, tokens):
        nix_words = stopwords.words('english')
        return [word for word in tokens if word not in nix_words]
