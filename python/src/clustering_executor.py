import numpy as np
import clustering_functions as cf
import sdcode_functions as sdcode
import minhash_functions as minhash
import networkx as nx
import math
class executor:
	def __init__(self, data, weber=1, d0=0, alg_name = 'all-connected'):
		self.data_num, self.col_num = data.shape
		if (self.data_num>3**self.col_num):
			self.executor = lowd_exec(data, weber, d0, alg_name)
		else:
			self.executor = highd_exec(data, weber, d0)
		self.scale_num = self.executor.scale_num
	def encoding(self):
		self.executor.encoding()
	def currentcodes(self, s):
		cur_code_set = self.executor.currentcodes(s)
		return cur_code_set
	def neighbors(self, c, cur_code_set):
		nbr_c_set = self.executor.neighbors(c, cur_code_set)
		return nbr_c_set
	def connection(self, c, nbr_c_set):
		self.executor.connection(c, nbr_c_set)
	def scale_rs(self,s):
		return self.executor.scale_rs(s)
	def all_rs(self):
		return self.executor.results

class highd_exec(executor):
	def __init__(self, data, weber, d0):
		self.data = data
		self.weber = weber
		self.min_sim = d0
		
		self.data_num, self.col_num = data.shape
		self.max_sim = self.col_num
		self.results = []
		if (d0>=1)|(d0<=0):
			self.min_sim = 1
		self.scale_num = cf.get_scale_num(weber, self.max_sim, self.min_sim)

	def encoding(self):
		for i,d in enumerate(self.data):
			self.data[i] = d/np.linalg.norm(d)
		self.code_set = sdcode.encoding(self.data, -1, is_equal=False)
	def int_encoding(self,l):
		for i,d in enumerate(self.data):
			self.data[i] = d/np.linalg.norm(d)
		max_dim = np.max(self.data, axis = 0)
		min_dim = np.min(self.data, axis = 0)
		gap_dim = (max_dim - min_dim)/l
		self.code_set = np.zeros([self.data_num, self.col_num],np.int)
		for i,d in enumerate(self.data):
			self.code_set[i] = (d-min_dim)/gap_dim
	def currentcodes(self, s):
		self.graph = nx.Graph()
		self.cur_jaccard = pow(1+self.weber, s)/self.max_sim
		cur_code_set, self.cur_lsh = minhash.build_lsh(self.code_set, self.cur_jaccard)
		return cur_code_set
		
	def neighbors(self, c, cur_code_set):
		temp = minhash.neighbors(c[1], self.cur_lsh)
		nbr_c_set = []
		for nc in temp:
			if minhash.jaccard(c[1], cur_code_set[nc][1])>=self.cur_jaccard:
				nbr_c_set.append(nc)
		return nbr_c_set
	def connection(self, c, nbr_c_set):
		for nc in nbr_c_set:
			self.graph.add_edge(c[0], nc)
	def scale_rs(self,s):
		if len(self.results)>s:
			return self.results[s]
		clusters = nx.connected_components(self.graph)
		rs = np.zeros([1,self.data_num])
		num = 1
		for cluster in clusters:
			rs[0,list(cluster)] = num
			num +=1
		self.results.append(rs)
		return rs
class lowd_exec(executor):
	def __init__(self, data, weber, d0, alg_name):
		self.data = data
		self.weber = weber
		self.max_dis = d0
		self.results = []
		self.alg_name = alg_name
		self.min_dis = cf.get_min_dis(data)
		self.data_num, self.col_num = data.shape
		if d0 <= 0:
			self.max_dis = np.max(data)-np.min(data)
		self.scale_num = cf.get_scale_num(weber, self.max_dis, self.min_dis)
		
	def encoding(self):
		self.code_set = sdcode.encoding(self.data, self.min_dis, is_equal=True)
		del self.data
		
	def currentcodes(self, s):
		self.graph = nx.Graph()
		self.nbr_num = 1
		cur_code_set = dict()
		if self.weber!=1:
			self.nbr_num = int(math.pow(1+self.weber, self.scale_num-s))
			if self.nbr_num >20:
				print('set weber =1 or reduce the d0')
				return 
		for i, c in enumerate(self.code_set):
			code = ''.join(c)
			if self.weber == 1:
				code = ''.join(c[0:(s+1)*self.col_num])
			if code in cur_code_set:
				cur_code_set[code].append(i)
			else:
				cur_code_set[code] = [i]
		self.cur_codes = cur_code_set
		return cur_code_set	
		
	def neighbors(self, c, cur_code_set):
		temp_set = sdcode.neighbors(c, self.col_num, self.nbr_num)
		nbr_c_set = []
		for nc in temp_set:
			if cur_code_set.has_key(nc):
				nbr_c_set.append(nc)
		return nbr_c_set
	
	def connection(self, c, nbr_c_set):
		for nc in nbr_c_set:
			self.graph.add_edge(c, nc)
	
	def scale_rs(self,s):
		if len(self.results)>s:
			return self.results[s]
		clusters = nx.connected_components(self.graph)
		rs = np.zeros([self.data_num, 1])
		num = 1
		for cluster in clusters:
			list = []
			for c in cluster:
				list.extend(self.cur_codes[c])
			rs[list] = num
			num +=1
		self.results.append(rs)
		return rs
