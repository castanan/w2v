# These pyspark scripts use rdd's and Spark MLlib

# The pyspark scripts under under w2v/ml-scripts use dataframe's and Spark ML and were added on March 2016 


## Instructions:

## Edit 

Open mllib-scripts/tweets-to-w2v.py and replace the data path 

datapath = '/Users/jorgecastanon/Documents/github/w2v/data/tweets.gz'

with your path:

datapath = 'YOUR-PATH-TO-REPO/w2v/data/tweets.gz'

## Generate the Word2Vec Model

Execute the following command sitting on YOUR-PATH-TO-REPO

~/Documents/spark-1.5.1/bin/spark-submit mllib-scripts/tweets-to-w2v.py data/filter.txt

replacing with your Spark home path:

YOUR-SPARK-HOME/bin/spark-submit mllib-scripts/tweets-to-w2v.py data/filter.txt

Note: this step generates files mllib-scripts/myW2Vmatrix.npy and mllib-scripts/myWordList.npy that are needed for the next step, so you may want to check if they were generated

## Cluster Twitter Terms with K-means 

Execute the following command sitting on YOUR-PATH-TO-REPO

~/Documents/spark-1.5.1/bin/spark-submit mllib-scripts/cluster-words.py

replace with 

YOUR-SPARK-HOME/bin/spark-submit mllib-scripts/cluster-words.py

Note: this step generates file mllib-scripts/myCluster.npy, so you may want to check if it was generated

We are ready to rock!

## Test you Word2Vec Matrix

Execute the following command

python mllib-scripts/dist-to-words.py

Your output should look like the following:

![image of Top 20 closest words to chritmas]
(https://github.com/castanan/w2v/blob/master/images/Top20ClosestWordsToChritstmas.png)

## Visualization of the Word2Vec Matrix via PCA

Execute the following command

~/Documents/spark-1.5.1/bin/spark-submit mllib-scripts/visualize-words.py

replace with 

YOUR-SPARK-HOME/bin/spark-submit visualize-words.py

You should get a cool 3D plot like this one:

![image of w2v-visual-via-pca]
(https://github.com/castanan/w2v/blob/master/images/Top30WordToChristmasVis.png)


## NOTES:

a. All these scripts run in local mode and need to be change to cluster mode for running them with large data sets of tweets. These scripts are a good starting point to play and understand the machine learning models. Nevertheless, these scripts need further work to be transformed into production and scalable code.

b. If your w2v matrix is too large, you may want to save it in hdfs, as well as all the large files (list of words, list of cluster labels,...).

c. A different list of keywords in filter.txt file may be useful for different applications.

d. Larger amount of tweets (or text documents) are needed to get an accurate Word2Vec model. 

e. The singular values of the Word2Vec matrix may be useful to choose the number of dimensions for each of the vectors associated with your word terms, #'s (hashtags) and @'s (Twitter handlers).