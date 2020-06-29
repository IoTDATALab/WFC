import numpy as np
import math
import matplotlib.pyplot as plt
import networkx as nx
import clustering_process as cl
import os


def main(inputdata, scale_num=-1, scale_lambda=1, alg_name='all_connected'):
    '''
	Clustering algorithm for Weber scale-space in both low and high dimension
	inputdata - (n*d)data for clustering ,when d <4 the dataset is low-dimensional,otherwise it's high-dimensional.
	scale_num - (int)the maximal scale number for SD-code, if it has been set -1, the scale number will be set by its precision(invalid in high dimensional dataset)
	scale_lambda-(float)the Weber factor of scale-space(invalid in low dimensional dataset)
	alg_name-(string)all-connected mean-shift dp
	'''
    cluster = cl.clustering_process(inputdata, scale_lambda)
    results = []
    codes = cluster.encoding_data(inputdata, scale_num)
    for s in range(0, cluster.scale_num):
        s_codes, neighbor_map = cluster.neighbor_map_built(codes, s, alg_name)
        G = nx.Graph()
        for i, c in enumerate(s_codes):
            nei_codes = cluster.get_neighbor(i, c, neighbor_map, s_codes, s)
            cluster.add_connection(i, c, neighbor_map, nei_codes, G)
        s_result = cluster.get_index2cluster(G, neighbor_map, s_codes, codes)
        results.append(s_result)
    return cluster.scale_num, results


if __name__ == "__main__":
    originaldata_path = os.path.abspath('..') + '//originaldata//s3.txt'
    outputdata_path = os.path.abspath('..') + '//outputdata//'
    data = np.loadtxt(originaldata_path, delimiter=',')
    data_num, dim_num = data.shape
    scale_num, scale_results = main(data, scale_num=8, alg_name='dp')
    for i_scale in range(0, scale_num):
        results = scale_results[i_scale]
        for i_class in set(list(results[:, 0])):
            indexs = np.where(results == i_class)
            c = [np.random.random(), np.random.random(), np.random.random()]
            plt.scatter(data[indexs, 0], data[indexs, 1], color=c)
        plt.title('Flame scale:' + str(i_scale))
        plt.show()
