from . import HtmlStripper


class StripHtml:

    def analyze(self, string):
        # TODO this works differently in ubuntu vs anaconda
        # on anaconda analyze('foo bar') returns 'foo bar'
        # on ubuntu the empty string is returned.
        raise Exception('not implemented')
        return HtmlStripper.strip_tags(string)
