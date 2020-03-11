from flask import Flask, request, render_template
from searcher import Searcher
import nltk


nltk.download('averaged_perceptron_tagger')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('stopwords')


app = Flask(__name__)
searcher = Searcher()


@app.route('/', methods=['GET'])
def main():
    return render_template('index.html', result="")


@app.route('/', methods=['POST'])
def process_query():
    query = request.form['query']
    relevants = searcher.search(query=query,
                                d_prefix_tree=searcher.dir_prefix_tree,
                                r_prefix_tree=searcher.rev_prefix_tree,
                                soundex_tree=searcher.soundex_index,
                                auxillary_index=searcher.aux_index)
    return render_template('index.html', result="relevants")


if __name__ == '__main__':
    # Launch server
    app.run(host="0.0.0.0")
