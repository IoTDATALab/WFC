import numpy as np
import math
import networkx as nx
from datasketch import MinHash, MinHashLSH

def build_lsh(code_set, jaccard):
	lsh = MinHashLSH(threshold = jaccard,num_perm=128)
	minhashes=[]
	for i, c in enumerate(code_set):
		m = minhashing(c)
		lsh.insert(str(i), m)
		minhashes.append([i,m])
	return minhashes,lsh
def minhashing(code):
	m = MinHash(num_perm=128)
	for i,c in enumerate(code):
		m.update((str(i)+'-'+str(c)).encode('utf8'))
	return m
def neighbors(code, lsh):
	neighbour_points = lsh.query(code)
	neighbour_points = [int(point) for point in neighbour_points]
	return neighbour_points
def jaccard(minhash, com_minhash):
	j = minhash.jaccard(com_minhash)
	return j