# Jorge Castanon, March 2016
# Data Scientist @ IBM

# run in terminal with comamnd sitting on YOUR-PATH-TO-REPO:
# ~/Documents/spark-1.5.1/bin/spark-submit ml-scripts/tweets-to-w2v.py
# Replace this line with:
# /YOUR-SPARK-HOME/bin/spark-submit ml-scripts/tweets-to-w2v.py

import numpy as np
import pandas as pd
import time
import math
from nltk.corpus import stopwords

from pyspark import SparkContext
from pyspark import Row
from pyspark.sql import SQLContext
from pyspark.ml.feature import Word2Vec
from pyspark.ml.clustering import KMeans

## spark, sql contexts
sc = SparkContext('local', 'train-w2v') #change to cluster mode when needed
sqlContext = SQLContext(sc)

datapath = '/Users/jorgecastanon/Documents/github/w2v/data/tweets.gz'
# Replace this line with:
# datapath = '/YOUR-PATH-TO-REPO/w2v/data/tweets.gz'

# next few lines read tweets from a compressed json file 
t0 = time.time()
tweets = sqlContext.read.json(datapath)
tweets.registerTempTable("tweets")
#totTweets = tweets.count()

#print "="*80
#print "Number of tweets read: ", totTweets # this line adds ~7 seconds (from ~24.5 seconds to ~31.5 seconds)
print "="*80
print "Elapsed time to read tweets as a data frame: ", time.time() - t0
print "="*80

# read keywords from w2v/data/filter.txt
filterPath = '/Users/jorgecastanon/Documents/github/w2v/data/filter.txt'
filter = pd.read_csv(filterPath,header=None)

# query tweets in english that contain at least one keyword
# construct SQL Command
t0 = time.time()
sqlString = "("
for substr in filter[0]: #iteration on the list of words to filter (at most 50-100 words)
    sqlString = sqlString+"text LIKE '%"+substr+"%' OR "
    sqlString = sqlString+"text LIKE '%"+substr.upper()+"%' OR "
sqlString=sqlString[:-4]+")"
sqlFilterCommand = "SELECT lang, text FROM tweets WHERE (lang = 'en') AND "+sqlString
# query
tweetsDF = sqlContext.sql(sqlFilterCommand)
#filtTweets = tweetsDF.count() # this line adds ~9 seconds (from ~0.72 seconds to ~9.42 seconds)

#print "="*80
#print "Number of filtered tweets: ", filtTweets
print "="*80
print "Elapsed time (seconds) to filter tweets of interest: ", time.time() - t0
print "="*80

#Parse and Remove Stop Words
tweetsRDD = tweetsDF.select('text').rdd

def parseAndRemoveStopWords(text):
    t = text[0].replace(";"," ").replace(":"," ").replace('"',' ')
    t = t.replace(',',' ').replace('.',' ').replace('-',' ')
    t = t.lower().split(" ")
    stop = stopwords.words('english')
    return [i for i in t if i not in stop]

tw = tweetsRDD.map(parseAndRemoveStopWords)

# Build Word2Vec Model with Spark ML
try:
	twDF = tw.map(lambda p: Row(text=p)).toDF()
except:
	print "For some reason, the first time to run the last command trows an error. The Error dissapears the second time that the command is run"

twDF = tw.map(lambda p: Row(text=p)).toDF()	

t0 = time.time()
word2Vec = Word2Vec(vectorSize=100, minCount=25, stepSize=0.1, inputCol="text", outputCol="result")
modelW2V = word2Vec.fit(twDF)
wordVectorsDF = modelW2V.getVectors()
print "="*80
print "Elapsed time(seconds) to build Word2Vec: ", time.time() - t0
print "="*80
print "Vocabulary Size: ", wordVectorsDF.count() 
print "="*80

# run K-means on top of the Word2Vec matrix:
t0=time.time()
vocabSize=wordVectorsDF.count()
K = int(math.floor(math.sqrt(float(vocabSize)/2)))
         # K ~ sqrt(n/2) this is a rule of thumb for choosing K,
         # where n is the number of words in the model
         # feel free to choose K with a fancier algorithm
           
dfW2V = wordVectorsDF.select('vector').withColumnRenamed('vector','features')
kmeans = KMeans(k=K, seed=1)
modelK = kmeans.fit(dfW2V)
labelsDF = modelK.transform(dfW2V).select('prediction').withColumnRenamed('prediction','labels')
print "="*80
print "Elapsed time(seconds) training K-means: ", time.time() - t0
print "="*80

#save models:
def toNumpy(df,colName):
    '''
    In .- spark dataframe with an specific column selected
    Out .- numpy array of that column
    '''
    dfCol = df.select(colName)
    return dfCol.map(lambda e: e[0]).collect()

w2vMatrix = toNumpy(wordVectorsDF,'vector')
np.save('w2vMatrix.npy',w2vMatrix)

words = toNumpy(wordVectorsDF,'word')
np.save('words.npy',words)

lables = toNumpy(labelsDF,'labels')
np.save('labels.npy',words)

sc.stop()



