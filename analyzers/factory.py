from .lowercase import Lowercase
from .porterstem import Porterstem
from .remove_stopwords import RemoveStopwords
from .strip_html import StripHtml
from .tokenize_numbers import TokenizeNumbers
from .alphanumeric import AlphaNumeric


def get_analyzer(key):
        if key == 'lowercase':
            return Lowercase()
        elif key == 'porterstem':
            return Porterstem()
        elif key == 'remove_stopwords':
            return RemoveStopwords()
        elif key == 'strip_html':
            return StripHtml()
        elif key == 'tokenize_numbers':
            return TokenizeNumbers()
        elif key == 'alpha_numeric':
            return AlphaNumeric()
        else:
            raise LookupError("unknown analyzer key {}".format(key))
