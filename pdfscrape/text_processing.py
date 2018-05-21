import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk import word_tokenize
from nltk import ngrams, bigrams
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, HashingVectorizer


STOPWORDS = stopwords.words('english')
PORTER = nltk.stem.porter.PorterStemmer()
SNOWBALL = nltk.stem.snowball.SnowballStemmer('english')
WORDNET = nltk.stem.WordNetLemmatizer()
METHODS = {'counter': CountVectorizer(),
           'tfid': TfidfVectorizer(),
           'hash': HashingVectorizer()}


def count_char_instances(data, col, substrings, force_lower = False):
    '''
    Count the number of occurances of each character/word in a string
    and create a column with the count.
    '''
    if force_lower:
        counter = lambda x, y: x.lower().count(y)
    else:
        counter = lambda x, y: x.count(y)
    for substring in substrings:
        data['sub_count_' + substring] = data[col].apply(lambda x: counter(x,substring))

    return data


def preprocess(text, stopwords = None, stemmer = None, lemmer = None):
    tokens = word_tokenize(text)
    workingIter = (w.lower() for w in tokens if w.isalpha())

    if stemmer is not None:
        workingIter = (stemmer.stem(w) for w in workingIter)

    if lemmer is not None:
        workingIter = (lemmer.lemmatize(w) for w in workingIter)

    if stopwords is not None:
        workingIter = (w for w in workingIter if w not in stopwords)
    #We will return a list with the stopwords removed
    return list(workingIter)

def all_text(df_col):
    all = ''
    for text in df_col.values:
        all += text if text else ''
    return all

def vectorize(data, text_col, method = 'counter', stopwords = None, stemmer = None, lemmer = None):
    vectorizer = METHODS[method]
    text = all_text(data[text_col])
    vectorizer.fit(preprocess(text, stopwords = stopwords, stemmer = stemmer, lemmer = lemmer))
    inv_map = {v: k for k, v in vectorizer.vocabulary_.items()}
    array = vectorizer.transform(data[text_col].tolist()).toarray()
    dataframe = pd.DataFrame(array)
    dataframe = dataframe.rename(columns = inv_map)
    return dataframe
    # return array
