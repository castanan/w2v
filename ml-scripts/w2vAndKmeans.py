# Jorge Castanon, March 2016
# Data Scientist @ IBM

# run in terminal with comamnd sitting on YOUR-PATH-TO-REPO:
# ~/Documents/spark-1.5.1/bin/spark-submit ml-scripts/w2vAndKmeans.py
# Replace this line with:
# /YOUR-SPARK-HOME/bin/spark-submit ml-scripts/w2vAndKmeans.py

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

## Spark and sql contexts
sc = SparkContext('local', 'train-w2v') #change to cluster mode when needed
sqlContext = SQLContext(sc)

datapath = '/Users/jorgecastanon/Documents/github/w2v/data/tweets.gz'
# Replace this line with:
# datapath = '/YOUR-PATH-TO-REPO/w2v/data/tweets.gz'

## Read Tweets 
t0 = time.time()
tweets = sqlContext.read.json(datapath)
tweets.registerTempTable("tweets")
timeReadTweets = time.time() - t0

## Read Keywords from w2v/data/filter.txt
filterPath = '/Users/jorgecastanon/Documents/github/w2v/data/filter.txt'
filter = pd.read_csv(filterPath,header=None)

## Filter Tweets
# construct SQL Command
t0 = time.time()
sqlString = "("
for substr in filter[0]: #iteration on the list of words to filter (at most 50-100 words)
    sqlString = sqlString+"text LIKE '%"+substr+"%' OR "
    sqlString = sqlString+"text LIKE '%"+substr.upper()+"%' OR "
sqlString=sqlString[:-4]+")"
sqlFilterCommand = "SELECT lang, text FROM tweets WHERE (lang = 'en') AND "+sqlString
tweetsDF = sqlContext.sql(sqlFilterCommand)
timeFilterTweets = time.time() - t0

## Parse and Remove Stop Words
tweetsRDD = tweetsDF.select('text').rdd
def parseAndRemoveStopWords(text):
    t = text[0].replace(";"," ").replace(":"," ").replace('"',' ')
    t = t.replace(',',' ').replace('.',' ').replace('-',' ')
    t = t.lower().split(" ")
    stop = stopwords.words('english')
    return [i for i in t if i not in stop]
tw = tweetsRDD.map(parseAndRemoveStopWords)

## Train Word2Vec Model with Spark ML
try:
	twDF = tw.map(lambda p: Row(text=p)).toDF()
except:
	print "For some reason, the first time to run the last command trows an error. The Error dissapears the second time that the command is run"
twDF = tw.map(lambda p: Row(text=p)).toDF()	
t0 = time.time()
word2Vec = Word2Vec(vectorSize=100, minCount=5, stepSize=0.025, inputCol="text", outputCol="result")
modelW2V = word2Vec.fit(twDF)
wordVectorsDF = modelW2V.getVectors()
timeW2V = time.time() - t0

## Train K-means on top of the Word2Vec matrix:
t0 = time.time()
vocabSize = wordVectorsDF.count()
K = int(math.floor(math.sqrt(float(vocabSize)/2)))
         # K ~ sqrt(n/2) this is a rule of thumb for choosing K,
         # where n is the number of words in the model
         # feel free to choose K with a fancier algorithm         
dfW2V = wordVectorsDF.select('vector').withColumnRenamed('vector','features')
kmeans = KMeans(k=K, seed=1)
modelK = kmeans.fit(dfW2V)
labelsDF = modelK.transform(dfW2V).select('prediction').withColumnRenamed('prediction','labels')
vocabSize = wordVectorsDF.count()
timeKmeans = time.time() - t0

sc.stop()


## Print Some Results
printResults = 1 # set t 
if (printResults):
    ## Read Tweets

    print "="*80
    print "Read Tweets..."
    print "Elapsed time (seconds) to read tweets as a data frame: ", timeReadTweets
    print "="*80

    ## Filter Tweets

    print "Filter Tweets..."
    print "Elapsed time (seconds) to filter tweets of interest: ", timeFilterTweets
    print "="*80

    ## Word2Vec
    
    print "Build Word2Vec Matrix..."
    print "Elapsed time (seconds) to build Word2Vec: ", timeW2V
    print "Vocabulary Size: ", vocabSize 
    print "="*80

    ## Kmeans
    
    print "Train K-means clustering..."
    print "Elapsed time (seconds) training K-means: ", timeKmeans
    print "Number of Clusters: ", K
    print "="*80

#save models:
saveModels = 0
if(saveModels):
    def toList(df,colName):
        dfCol = df.select(colName)
        return dfCol.map(lambda e: e[0]).collect()

    w2vMatrix = toList(wordVectorsDF,'vector')
    np.save('w2vMatrix.npy',w2vMatrix)

    words = toList(wordVectorsDF,'word')
    np.save('words.npy',words)

    lables = toList(labelsDF,'labels')
    np.save('labels.npy',words)



