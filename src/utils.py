import pandas as pd
from nltk.tokenize import TreebankWordTokenizer



def csv_to_df(path_to_csv :str) -> pd.DataFrame : 
    """Return a dataframe from a path to a csv file"""
    df=pd.read_csv(path_to_csv)
    return df

def tokenize_sentence(sentence:str) -> list:
    """ Tokenize and return a sentence by blank beteween words"""
    tokenizer = TreebankWordTokenizer()
    tokenized_sentence=[w.lower() for w in tokenizer.tokenize(sentence)]
    return tokenized_sentence


def extract_word_from_sentence(sentence:str, keywords:list) -> list:
    """ Return a list of words mentioned in a tokenized sentence"""
    word_tokens = tokenize_sentence(sentence)
    keywords_mentioned = [w for w in keywords if w.lower() in word_tokens]
    return keywords_mentioned 


 