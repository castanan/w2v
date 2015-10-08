# Jorge Castanon, October 2015
# Data Scientist @ IBM

# run in terminal with the command
# $ python dist-to-words.py

import numpy as np
from math import sqrt

word = 'christmas' # word of interest
nwords = 10   # number of words close to 'word' to print

Feat = np.load('myW2Vmatrix.npy')  
words = np.load('myWordList.npy')

Nw = words.shape[0] #total number of words
ind_star = np.where(word == words)
wstar = Feat[ind_star,:][0][0]     ## vector corresponding to the chosen 'word'
nwstar = sqrt(np.dot(wstar,wstar)) ## norm of wstar

dist = np.zeros(Nw)
i = 0
for w in Feat:
    num = sqrt(np.dot(w,w))*nwstar
    dist[i] = np.dot(wstar,w)/num
    i = i + 1

indexes = np.argpartition(dist,-(nwords+1))[-(nwords+1):]
d = []
for j in range(nwords+1):
    d.append(( words[indexes[j]], dist[indexes[j]]))

i = 0
for elem in sorted(d,key=lambda x: x[1],reverse=True):
    if (i==1):
	print "Most similar words to", word+":"
    print i, ".-", elem[0] #, elem[1]
    i = i + 1	


