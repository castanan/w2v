# Building a Word2Vec Model with Twitter Data

For an end to end tutorial on how to build models on IBM's [Data Science Experience](http://datascience.ibm.com/), please chech [this repo](https://github.com/IBMDataScience/word2vec).

## To read my blog, please click [here](http://www.ibmbigdatahub.com/blog/spark-based-machine-learning-capturing-word-meanings).

![image of pyspark shell]
(https://github.com/castanan/w2v/blob/master/images/w2v-ibm-design.png)

## Pre-reqs: install Python, numpy and Apache Spark 

I.) Installing Anaconda installs Python, numpy, among other Python packages. If interested go here https://www.continuum.io/downloads

II.) Download and Install Apache Spark go here: http://spark.apache.org/downloads.html

This steps were useful for me to install Spark 1.5.1 on a Mac https://github.com/castanan/w2v/blob/master/Install%20Spark%20On%20Mac.txt

III.) Added a notebook here  https://github.com/castanan/w2v/blob/master/mllib-scripts/Word2Vec with Twitter Data usign Spark RDDs.ipynb
 and the good news are that Spark comes with Jupyter + Pyspark integrated. This notebook can be invoked from the shell by typing the command:
IPYTHON_OPTS="notebook" ./bin/pyspark
if you are sitting on YOUR-SPARK-HOME.

## Make sure that your pyspark is working

I.) Go to your spark home directory

cd YOUR-SPARK-HOME/bin

II.) Open a pyspark shell by typing the command

./pyspark

or Pyspark with Jupyter by typing the command

IPYTHON_OPTS="notebook" ./bin/pyspark

III.) print your spark context by typing sc in the pyspark shell, you should get something like this:

![image of pyspark shell]
(https://github.com/castanan/w2v/blob/master/images/pyspark-shell.png)

## Get the Repo

git clone https://github.com/castanan/w2v.git

cd /YOUR-PATH-TO-REPO/w2v 

## Get the Data

Get some tweets from December 23, 2014 from here: 

https://www.dropbox.com/sh/82rrk8di2nouf0x/AAAIMc6J9rWpu08UBLhLbHXEa?dl=0 

Note: there is no need to uncompress the file, just download the tweets.gz file and save it on the repo /YOUR-PATH-TO-REPO/w2v/data/.  

## There are 2 options to perform the Twitter analysis: 

1) (suggested) use dataframes and Spark ML (March 2016). see ml-scripts/README.md 

2) use rdd's and Spark MLlib (October 2015). see mllib-scripts/README.md



