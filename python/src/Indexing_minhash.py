import numpy as np
import math
import Indexing_sdcode as sdcode
import networkx as nx
from datasketch import MinHash, MinHashLSH


def minhashBuild(code):
    m = MinHash(num_perm=128)
    for i, c in enumerate(code):
        m.update((str(i) + '-' + str(c)).encode('utf8'))
    return m


def minhashlsh_build(code_data, jaccard):
    lsh = MinHashLSH(threshold=jaccard, num_perm=128)
    minhashes = []
    for index, (code, content) in enumerate(code_data):
        m = minhashBuild(code)
        lsh.insert(str(index), m)
        minhashes.append((m, content))
    return minhashes, lsh


def get_jaccard(scale_lambda, dim_num, scale_num):
    sigma_similarity = dim_num - (scale_lambda + 1) ** scale_num
    jaccard = sigma_similarity / (2 * dim_num - sigma_similarity + 0.0)
    return jaccard


def neighbourhoodFind(index, minhash, lsh, scale, scale_lambda, dim_num, minhashes):
    neighbour_points = lsh.query(minhash[0])
    neighbour_points = [int(point) for point in neighbour_points]
    neighbour_list = []
    if minhash[0].seed == -1:
        return []
    jaccard = get_jaccard(scale_lambda, dim_num, scale)
    for i in neighbour_points:
        m = minhashes[i]
        m[0].seed = 1
        j = minhash[0].jaccard(m[0])
        if j >= jaccard:
            neighbour_list.append(i)
    for i in neighbour_list:
        minhashes[i][0].seed = -1
    minhashes[index] = (minhash[0], set(neighbour_list))

    return neighbour_list


def nei_generalized(neighbour_list, code_data):
    neighbour_codes = [code_data[a][0] for a in neighbour_list]
    new_code = np.median(neighbour_codes, axis=0)
    return (new_code, set(neighbour_list))
