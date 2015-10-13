# Jorge Castanon, October 2015
# Data Scientist @ IBM

# run in terminal with the command
# $ python dist-to-words.py

import numpy as np
from math import sqrt

word = 'christmas' # word of interest
nwords = 20   # number of words close to 'word' to print

Feat = np.load('myW2Vmatrix.npy')  
words = np.load('myWordList.npy')
labels = np.load('myClusters.npy')

Nw = words.shape[0]                # total number of words
ind_star = np.where(word == words) # find index of the chosen word
wstar = Feat[ind_star,:][0][0]     # vector corresponding to the chosen 'word'
nwstar = sqrt(np.dot(wstar,wstar)) # norm of vector corresponding to the chosen 'word'

dist = np.zeros(Nw) # initialize vector of distances
i = 0
for w in Feat: # loop to compute cosine distances 
    den = sqrt(np.dot(w,w))*nwstar  # compute denominator of cosine distance
    dist[i] = np.dot(wstar,w)/den   # compute cosine distance to each word
    i = i + 1

indexes = np.argpartition(dist,-(nwords+1))[-(nwords+1):]
di = []
for j in range(nwords+1):
    di.append(( words[indexes[j]], dist[indexes[j]], labels[indexes[j]] ) )

i = 0
for elem in sorted(di,key=lambda x: x[1],reverse=True):
    if (i==1):
	print "Most Similar Words to", word," and Cluster Labels:"
    print i, ".-", elem[0], elem[2]
    i = i + 1	
    



