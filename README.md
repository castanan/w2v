# Building a Word2Vec Model with Twitter Data

## Blog: http://www.spark.tc/building-a-word2vec-model-with-twitter-data/

## First install Python, numpy and Apache Spark 

I.) Installing Anaconda installs Python, numpy, among other Python packages. If interested go here https://www.continuum.io/downloads

II.) Download Apache Spark go here: http://spark.apache.org/downloads.html

## Get the Repo

git clone https://github.com/castanan/w2v.git

cd /YOUR-PATH-TO-REPO/w2v 

## Get the Data

Get some tweets from chritmas eve 2014 from here: 

https://www.dropbox.com/sh/82rrk8di2nouf0x/AAAIMc6J9rWpu08UBLhLbHXEa?dl=0 

Note: there is no need to uncompress the file, just download the tweets.gz file and save it on the repo /YOUR-PATH-TO-REPO/w2v.  

## Edit 

Open tweets-to-w2v.py and replace the data path line (line 54)

datapath = '/Users/jorgecastanon/Documents/github/w2v/tweets.gz'

with your path:

datapath = 'YOUR-PATH-TO-REPO/w2v/tweets.gz'

## Generate the Word2Vec Model

Execute the following command

~/Documents/spark-1.5.1/bin/spark-submit tweets-to-w2v.py filter.txt

replacing with your Spark home path:

YOUR-SPARK-HOME/bin/spark-submit tweets-to-w2v.py filter.txt

Note: this step generates files myW2Vmatrix.npy and myWordList.npy that are needed for the next step, so you may want to check if they were generated

## Cluster Twitter Terms with K-means 

Execute the following command

~/Documents/spark-1.5.1/bin/spark-submit cluster-words.py

replace with 

YOUR-SPARK-HOME/bin/spark-submit cluster-words.py

Note: this step generates file myCluster.npy, so you may want to check if it was generated

We are ready to rock!

## Test you Word2Vec Matrix

Execute the following command

python dist-to-words.py


## NOTES:

a. All these scripts run in local mode and need to be change to cluster mode for running them with large data sets of tweets

b. If your w2v matrix is too large, you may want to save it in hdfs 

c. A different list of keywords in filter.txt file may be useful for different applications

d. Larger amount of tweets (or text documents) are needed to get an accurate Word2Vec model 

e. The singular values of the Word2Vec matrix may be useful to choose the number of dimensions for each of the vectors associated with your word terms, #'s (hashtags) and @'s (Twitter handlers) 

## FUTURE WORK:

a. Use the dataframe API to improve efficiency

