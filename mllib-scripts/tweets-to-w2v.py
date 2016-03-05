#!/usr/bin/python

# Jorge Castanon, October 2015
# Data Scientist @ IBM

# run in terminal with comamnd sitting on YOUR-PATH-TO-REPO:
# ~/Documents/spark-1.5.1/bin/spark-submit mllib-scripts/tweets-to-w2v.py data/filter.txt
# Replace this line with:
# /YOUR-SPARK-HOME/bin/spark-submit mllib-scripts/tweets-to-w2v.py data/filter.txt

# filter.txt contains a list of words of interest and may vary 
# depending on the application. In this example, filter.txt contain
# chritmas related words: santa, claus, rudolf,...

import os, sys, codecs, json
import numpy as np
from math import sqrt

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

# read the keywords to filter tweets
# in our example, these keywords are stored in 'filter.txt'
def readFilters(filterpath):
    filters = set()
    f = open(filterpath, 'r')
    for line in f:
        line = codecs.decode(line, "utf-8")
        line = line.strip()
        if len(line) == 0: continue
        filters.add(line.lower())
    f.close()
    return filters

# parse text of each tweets in english that contains at least one keyword
def process(filters):
    def realProcess(line):
        key = 'text'
        try:
            t = json.loads(line)
            if key not in t or t['lang']!='en': return None
            value = t[key].lower()
            # match with filters
            found = False
            for ff in filters:
                if ff in value:
                    return t
                    break
        except Exception as e:
            return None
        return None
    return realProcess

def main(filterpath):
    filters = readFilters(filterpath)
    print >>sys.stderr, "Loaded %d filters" % len(filters)
    
    ## spark context
    sc = SparkContext('local', 'train-w2v') #change to cluster mode when needed

    datapath = '/Users/jorgecastanon/Documents/github/w2v/data/tweets.gz'
    # Replace this line with:
    # datapath = '/YOUR-PATH-TO-REPO/w2v/data/tweets.gz'

    data = sc.textFile(datapath)
    totaltw = data.count()

    print "\n================================================="
    print "Number of total tweets processed: ", totaltw
    print "=================================================\n"

    
    # the next line filters tweets of interest
    tweets = data.map(process(filters)).filter(lambda x: x != None).map(lambda t: t['text'])
    twcount = tweets.count()
    # the next line cleans unwanted characters and transform text to lower case
    tweets = tweets.map(lambda x: x.replace(";"," ").replace(":"," ").replace('"',' ').replace('-',' ').replace(',',' ').replace('.',' ').lower())
    # the next line breaks tweets into words
    tweets = tweets.map(lambda row: row.split(" ")) 


    print "\n================================================="
    print "Number of filtered tweets used: ", twcount
    print "=================================================\n"

    ## train NN model with word2vec
    word2vec = Word2Vec()
    model = word2vec.fit(tweets) # complexity

    ## Get the list of words in the w2v matrix
    vocabsize = 10000       # max vocabulary size is at most 10K words
    any_word = 'christmas'  # can be any word in your model
    # the next line finds the top 'vocabsize' words to 'any_word' 
    tmp_list = model.findSynonyms(any_word, vocabsize-1) 
    # the newt few lines define the list of words in the model
    list_words = []
    for l in tmp_list:
        list_words.append(l[0])
    list_words.append(any_word)

    nwords = len(list_words)
    nfeatures = model.transform(any_word).array.shape[0]

    print "\n================================================="
    print "Number of words in the model:", nwords
    print "================================================="
    print "Number of features per word: ", nfeatures
    print "=================================================\n"

    ## Construct the feature matrix, each row vector is associated to each word in 'list_words'
    feature_matrix = [] 
    for word in list_words:
        # model.transform : word => vectors 
        # this function maps a word to its numerical vector
        feature_matrix.append(model.transform(word).array)
    
    ## save W2V matrix and the list of words 
    np.save('mllib-scripts/myW2Vmatrix.npy',feature_matrix)
    np.save('mllib-scripts/myWordList.npy',list_words)

    sc.stop()
    return
    print >> sys.stderr, cnt, cnt_out, cnt_err

def checkInput():
    if len(sys.argv) != 2:
        print "Usage: " + os.path.basename(sys.argv[0]) + " filter_file_path"
        sys.exit()
    return sys.argv[1]

if __name__ == '__main__':
    filterpath = checkInput()
    main(filterpath)
