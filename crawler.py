from searcher import Searcher
from datetime import datetime
import requests, bs4, re


class Crawler:
    def __init__(self):
        self.data, self.old = {}, []

    def send_data(self, searcher: Searcher):
        searcher.append_aux_index(searcher.aux_index, self.data)

    def get_old_docs(self, old_docs: list):
        self.old += old_docs
        self.old = list(set(self.old))

    def get_new_data(self, url, searcher: Searcher):
        content = requests.get(url=url).content
        ts = datetime.now().timestamp() * 1000
        soup = bs4.BeautifulSoup(str(content))
        words = []
        for unit in soup.find_all(text=True):
            if not isinstance(unit,
                              bs4.element.Comment) and not re.match(r"[\s\r\n]",
                                                                    unit) and unit.parent.name not in [
                'style', 'script', '[document]', 'head', 'title', 'meta'
            ]:
                tokens = searcher.preprocess(unit, True)
                if len(tokens) > 0:
                    words.append(tokens)

        # convert to dict
        for word in words:
            if self.data.get(word) is None:
                self.data[word] = dict()
                self.data[word][ts] = [ts * 10**12]
            else:
                self.data[word][ts] += [ts * 10**12]
        return 0
