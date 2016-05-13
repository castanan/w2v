
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
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import PCA

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def topNwordsToPlot(dfComp,wordVectorsDF,word,nwords):

    compX = np.asarray(dfComp.map(lambda vec: vec[0][0]).collect())
    compY = np.asarray(dfComp.map(lambda vec: vec[0][1]).collect())
    compZ = np.asarray(dfComp.map(lambda vec: vec[0][2]).collect())

    words = np.asarray(wordVectorsDF.select('word').toPandas().values.tolist())
    Feat = np.asarray(wordVectorsDF.select('vector').rdd.map(lambda v: np.asarray(v[0])).collect())

    Nw = words.shape[0]                # total number of words
    ind_star = np.where(word == words) # find index associated to 'word' 
    wstar = Feat[ind_star,:][0][0]     # vector associated to 'word'
    nwstar = math.sqrt(np.dot(wstar,wstar)) # norm of vector assoicated with 'word'

    dist = np.zeros(Nw) # initialize vector of distances
    i = 0
    for w in Feat: # loop to compute cosine distances between 'word' and the rest of the words 
        den = math.sqrt(np.dot(w,w))*nwstar  # denominator of cosine distance
        dist[i] = abs( np.dot(wstar,w) )/den   # cosine distance to each word
        i = i + 1

    indexes = np.argpartition(dist,-(nwords+1))[-(nwords+1):]
    di = []
    for j in range(nwords+1):
        di.append(( words[indexes[j]], dist[indexes[j]], compX[indexes[j]], compY[indexes[j]], compZ[indexes[j]] ) )

    result=[]
    for elem in sorted(di,key=lambda x: x[1],reverse=True):
        result.append((elem[0][0], elem[2], elem[3], elem[4]))
    
    return pd.DataFrame(result,columns=['word','X','Y','Z'])