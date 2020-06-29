import numpy as np
import math
import matplotlib.pyplot as plt
import Indexing_sdcode as sdcode
import Indexing_minhash as minhash
import networkx as nx
from datasketch import MinHash, MinHashLSH


class clustering_process():
    def __init__(self, inputdata, scale_lambda=1):
        data_num, dim_num = inputdata.shape
        self.scale_lambda = scale_lambda
        self.dim_num = dim_num
        self.data_num = data_num

    def encoding_data(self, inputdata, scale_num):
        encoded_data = []
        if self.dim_num < 4:
            if scale_num == -1:
                scale_num = sdcode.get_scale_num(inputdata, 1)
                print scale_num
            encoded_data = sdcode.encoding_sdcode(inputdata, scale_num)
        else:
            encoded_data_temp = sdcode.encoding_sdcode(inputdata, 1)
            encoded_data = [(map(int, a), set([b])) for b, a in enumerate(encoded_data_temp)]
            scale_num = int(math.log(self.dim_num, 1 + self.scale_lambda))
        self.scale_num = scale_num
        return encoded_data

    def neighbor_map_built(self, encoded_data, scale, alg_name):
        neighbor_map = {}
        scale_codes = {}
        self.alg_name = alg_name
        if self.dim_num < 4:
            scale_codes, neighbor_map = sdcode.sdcode_neighbor_build(encoded_data, scale, self.dim_num)
            if (alg_name == "dp") | (alg_name == "mean-shift"):
                neighbor_map = sdcode.get_kernel_map(self.dim_num, self.scale_num - scale, neighbor_map, self.alg_name)
        else:
            jaccard = minhash.get_jaccard(self.scale_lambda, self.dim_num, scale)
            scale_codes, neighbor_map = minhash.minhashlsh_build(encoded_data, jaccard)
        return scale_codes, neighbor_map

    def get_neighbor(self, index, code, neighbor_map, scale_codes, scale, ):
        nei_codes = []
        if self.dim_num < 4:
            nei_codes_temp = sdcode.sdcoding_nei_find(code, self.dim_num, 1, self.scale_num - scale)
            for nei_code in nei_codes_temp:
                if neighbor_map.has_key(nei_code):
                    nei_codes.append(nei_code)
        else:
            nei_codes = minhash.neighbourhoodFind(index, code, neighbor_map, scale, self.scale_lambda, self.dim_num,
                                                  scale_codes)
        return nei_codes

    def add_connection(self, index, code, neighbor_map, nei_codes, G):
        if self.dim_num < 4:
            if self.alg_name == 'all_connected':
                for nei_code in nei_codes:
                    G.add_edge(code, nei_code)
            else:
                max_code = sdcode.get_maxkernel_code(neighbor_map, code, nei_codes)
                if self.alg_name == 'dp':
                    G.add_edge(code, max_code)
                elif self.alg_name == 'mean-shift':
                    for nei_code in nei_codes:
                        dis = sdcode.sdcoding_code_distance(max_code, nei_code, self.dim_num)
                        if dis <= 1:
                            G.add_edge(max_code, nei_code)
        else:
            for nei_code in nei_codes:
                G.add_edge(index, nei_code)

    def get_index2cluster(self, G, neighbor_map, scale_codes, codes):
        results = np.zeros([self.data_num, 1])
        clusters = nx.connected_components(G)
        if self.dim_num < 4:
            cluster_num = 1
            if self.alg_name == 'all_connected':
                for cluster in clusters:
                    points = []
                    for code in cluster:
                        points.extend(neighbor_map[code])
                    results[points] = cluster_num
                    cluster_num += 1
            elif (self.alg_name == 'dp') | (self.alg_name == 'mean-shift'):
                for cluster in clusters:
                    points = []
                    for code in cluster:
                        points.extend(neighbor_map[code][1])
                    results[points] = cluster_num
                    cluster_num += 1
        else:
            cluster_num = 1
            new_codes = []
            for cluster in clusters:
                pointset = set()
                for code in cluster:
                    pointset = pointset | scale_codes[code][1]
                    l = list(scale_codes[code][1])
                    if len(l) > 1:
                        new_code = minhash.nei_generalized(l, codes)
                        new_codes.append(new_code)
                results[list(pointset)] = cluster_num
                cluster_num += 1
            codes = new_codes
        return results
