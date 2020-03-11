import rethinkdb, re, nltk, os, bs4
from jellyfish import soundex as jsoundex
from crawler import Crawler


class Searcher:
    def __init__(self):
        self.r = rethinkdb.RethinkDB()
        self.conn = self.r.connect(db='IR')
        self.conn.use('IR')
        if 'index' not in list(self.r.table_list().run(self.conn)):
            self.r.table_create('index', primary_key='word').run(self.conn)

        self.dir_prefix_tree, self.rev_prefix_tree = {None: {}}, {None: {}}
        self.aux_index, self.soundex_index = {}, {}

        self.produce_stuff(path='../data/', dir_tree=self.dir_prefix_tree,
                           rev_tree=self.rev_prefix_tree, sound_index=self.soundex_index)

    # https://pythonspot.com/nltk-stop-words/
    @staticmethod
    def remove_stop_word(tokens):
        return [token for token in tokens if token not in set(nltk.corpus.stopwords.words('english'))]

    def preprocess(self, text, lemmatize=True):
        # src https://stackoverflow.com/questions/24232702/python-nltk-how-to-lemmatize-text-include-verb-in-english
        def lemmatization(tokens):
            wnl = nltk.stem.WordNetLemmatizer()
            res = [wnl.lemmatize(i, j[0].lower()) if j[0].lower() in ['a', 'n', 'v'] else wnl.lemmatize(i) for i, j
                   in
                   nltk.pos_tag(tokens)]
            return res

        text = nltk.word_tokenize(str(re.sub(r"[^A-Za-z *]+", '', text)).lower())
        if lemmatize:
            text = lemmatization(text)
        return self.remove_stop_word(text)

    # Read markup by <filepath>
    def get_collection(self, filepath: str, lemmatize=True):
        collection = list()
        with open(filepath, 'r') as f:
            soup = bs4.BeautifulSoup(f.read())
            for unit in soup.find_all(text=True):
                if not isinstance(unit,
                                  bs4.element.Comment) and not re.match(r"[\s\r\n]",
                                                                        unit) and unit.parent.name not in [
                    'style', 'script', '[document]', 'head', 'title', 'meta'
                ]:
                    tokens = self.preprocess(unit, lemmatize)
                    if len(tokens) > 0:
                        collection.append(tokens)
        return filepath, collection

    # AUXILLARY INDEX
    @staticmethod
    def append_aux_index(auxillary_index: dict, words: dict):
        for key in words.keys():
            if auxillary_index.get(key) is None:
                auxillary_index[key] = words.get(key)
            else:
                auxillary_index[key] += words.get(key)
        return 0

    # SOUNDEX INDEX
    @staticmethod
    def append_soundex_index(index: dict, collection: list):
        for elem in collection:
            code = str(jsoundex(elem))
            if index.get(code) is None:
                index[code] = []
            else:
                index[code] += elem

    @staticmethod
    def append_prefix_trees(collection: list, dir_tree: dict, rev_tree: dict):
        # Append to direct prefix tree
        for doc_id in range(len(collection)):
            for element in collection[doc_id]:
                # start from root
                ptr = dir_tree.get(None)
                # insert in tree if necessary
                for char in element:
                    if ptr.get(char) is None:
                        ptr[char] = {}
                    ptr = ptr.get(char)
                # after finish, add doc_id
                if ptr.get('$') is None:
                    ptr['$'] = list()
                ptr.get('$').append(doc_id)
        # Append to reversed prefix tree
        for doc_id in range(len(collection)):
            for element in collection[doc_id]:
                # start from root
                ptr = dir_tree.get(None)
                # insert in tree if necessary
                for char in element[::-1]:
                    if ptr.get(char) is None:
                        ptr[char] = {}
                    ptr = ptr.get(char)
                # after finish, add doc_id
                if ptr.get('$') is None:
                    ptr['$'] = list()
                ptr.get('$').append(doc_id)
        return 0

    # Insert new data to DB
    def update_inverted_index(self, new_collection: list, collection_filepath: str):
        # insert shit to DB
        for doc_id in range(len(new_collection)):
            for element in new_collection[doc_id]:
                document = self.r.table('index').get(element).run(self.conn)
                if document is None:
                    self.r.table('index').insert({
                        'word': element, 'loc': {collection_filepath: [doc_id]}
                    }).run(self.conn)
                else:
                    cur_loc = document.get('loc')
                    if cur_loc.get(collection_filepath) is None:
                        cur_loc[collection_filepath] = [doc_id]
                    else:
                        cur_loc[collection_filepath] += [doc_id]
                    self.r.table('index').get(element).replace({'word': element, 'loc': cur_loc}).run(self.conn)
        return 0

    # Fill up all stuff
    def produce_stuff(self, path: str = './data/', dir_tree=None, rev_tree=None, sound_index=None):
        if sound_index is None:
            sound_index = {}
        if rev_tree is None:
            rev_tree = {}
        if dir_tree is None:
            dir_tree = {}
        for entry in os.scandir(path):
            if entry.is_file() and entry.name.find('.sgm') != -1:
                path, data = self.get_collection(entry.path, False)
                print(path, end=': ')

                # Update inverted index (with document inex only)
                self.update_inverted_index(new_collection=data, collection_filepath=path.split('.sgm')[0][-3:])
                print('done INV;', end=' ')

                # Update both trees
                self.append_prefix_trees(collection=data, dir_tree=dir_tree, rev_tree=rev_tree)
                print('done TRS;', end=' ')

                # Update soundex index
                self.append_soundex_index(sound_index, data)
                print('done SND.')
                return 0

    def remove_old(self, crawler: Crawler, old_docs: list):
        crawler.get_old_docs(old_docs)

    def merge(self):
        for key in self.aux_index.keys():
            doc = self.r.table('index').get(key).run(self.conn)
            if doc is not None:
                self.r.table('index').get(key).delete().run(self.conn)
            self.r.table('index').insert({'word': key, 'loc': self.aux_index.get(key)}).run(self.conn)
        return 0

    def search(self, query: str, d_prefix_tree: dict, r_prefix_tree: dict, soundex_tree: dict, auxillary_index: dict):

        def correct_word(dumb: str, d_prefix_tree: dict, sound_index: dict):
            # dumb check. tu-du-bum.
            if len(dumb) < 2:
                return []

            code = str(jsoundex(dumb))
            sound_like = sound_index.get(code)
            if sound_like is None or len(sound_like) == 0:
                return []
            return sound_like

        def parse_wildcard(suspect: str, d_prefix_tree: dict, r_prefix_tree: dict):
            def get_all_paths(struct: dict, storage: str):
                result = []
                for each in list(struct.keys()):
                    result.append(each)
                    if each is not '$':
                        result += get_all_paths(struct.get(each), storage + each)
                return result

            # https://en.wikipedia.org/wiki/Levenshtein_distance
            def get_levelstein_distance(seq1: str, seq2: str):
                def comp(str1: str, str2: str):
                    return 0 if str1 == str2 else 1

                def strange(len1: int, len2: int):
                    if len1 == len2 == 0:
                        return 0
                    elif len1 > 0 and len2 == 0:
                        return len1
                    elif len2 > 0 and len1 == 0:
                        return len2
                    else:
                        return min([strange(len1, len2 - 1) + 1,
                                    strange(len1 - 1, len2) + 1,
                                    strange(len1 - 1, len2 - 1) + comp(seq1[len1 - 1], seq2[len2 - 1])
                                    ])

                return strange(len(seq1), len(seq2))

            suspect, res = suspect.split('*'), list()
            # case 0: *
            if suspect[0] == suspect[1] == '':
                return [None]

            # case 1: begin*
            elif suspect[0] != "":
                # go through tree
                ptr = d_prefix_tree.get(None)
                for char in suspect[0]:
                    ptr = ptr.get(char)
                # get all words
                successors = get_all_paths(ptr, '')
                for s in range(1, len(successors)):
                    successors[0] += successors[s]
                successors = successors[0].split('$')

                # case 2: begin*end
                if suspect[1] != '':
                    for each in successors:
                        spl = each.split(suspect[1])
                        if len(spl) > 1 and spl[1] == '':
                            res.append(suspect[0] + each)
                else:
                    for each in successors:
                        res.append(suspect[0] + each)

            # case 3: *end: via reverse prefix tree
            elif suspect[0] == '' and suspect[1] != '':
                # go through tree
                ptr = r_prefix_tree.get(None)
                for char in suspect[1][::-1]:
                    ptr = ptr.get(char)
                # get all words
                successors = get_all_paths(ptr, '')
                for s in range(1, len(successors)):
                    successors[0] += successors[s]
                successors = successors[0].split('$')
                # append 'em to result
                for each in successors:
                    res.append(suspect[0] + each[::-1])

            # finally!
            return res

        def convert_word_to_set(word: str) -> set:
            document = self.r.table('index').get(word).run(self.conn)
            document = document.get('loc')
            # str path -> int[] ids
            result = list()
            for key in document.keys():
                for doc_id in document.get(key):
                    result.append(int(key) * 10 ** 12 + int(doc_id))
            return set(result)

        def str_in_tree(word: str, prefix_tree: dict):
            ptr = prefix_tree.get(None)
            for char in ptr:
                ptr = ptr.get(char)
            return ptr.get('$') is not None

        def get_key_in_index(key: str):
            doc = self.r.table('index').get(key).run(self.conn)
            return doc

        upd_query, relevant_documents = self.preprocess(query), list()
        if len(upd_query) == 0:
            return []
        if len(upd_query) == 1:
            query = [query]

        # Scan query
        print('scan:', query)
        for q in range(len(query)):
            if '*' in query[q]:
                print(query[q], 'with *', end='->')
                query[q] = parse_wildcard(suspect=query[q], d_prefix_tree=d_prefix_tree, r_prefix_tree=r_prefix_tree)
                print(query[q])
            elif not str_in_tree(query[q], d_prefix_tree):
                print(query[q], 'not in tree')
                query[q] = correct_word(dumb=query[q], d_prefix_tree=d_prefix_tree, sound_index=soundex_tree)

        # Remove stop words
        print('rm:', query)
        for q in range(len(query)):
            if type(query[q]) is list and query[q][0] is not None:
                query[q] = self.remove_stop_word(query[q])
            else:
                query[q] = self.remove_stop_word([query[q]])

        # Convert all stuff to sets
        print('conv:', query)
        for q in range(len(query)):
            # multiple choice: OR
            if type(query[q]) is list:
                # Special case: any word can be here
                if query[q][0] is None:
                    query[q][0] = set(range(self.r.table('index').count().run(self.conn)))
                # Nothing found
                elif get_key_in_index(query[q][0]) is None:
                    query[q][0] = set([])
                # Regular case
                else:
                    for q_i in range(len(query[q])):
                        query[q][q_i] = convert_word_to_set(query[q][q_i])
                    for q_i in range(1, len(query[q])):
                        query[q][0] = query[q][0].union(query[q][q_i])
            else:
                query[q] = convert_word_to_set(query[q])

        # Intersect all sets: AND
        for q in range(1, len(query)):
            query[0] = query[0].intersection(query[q])
        return list(query[0])
