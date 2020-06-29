import clustering_executor as ce
import matplotlib.pyplot as plt
import math
def main(data, Lambda):
	#Initialize the clustering instance.
	cluster = ce.executor(data, Lambda)
	#Encode all input data.
	cluster.encoding()
	#Clustering at each scale s.
	print(cluster.scale_num)
	for s in range(0, cluster.scale_num):
		##Compute all codes at current scale s.
		cur_code_set = cluster.currentcodes(s)
		##Operations for each code c.
		for c in cur_code_set:
			###Compute neighbor set of c.
			nbr_c_set = cluster.neighbors(c, cur_code_set)
			###Establish connections in c's neighbor set.
			cluster.connection(c, nbr_c_set)
		##Compute the clustering result at scale s.
		cluster.scale_rs(s)
	return cluster.all_rs()
def main_face(data, tag_data, weber, d0=0, alg_name='all_connected'):
	cluster = ce.executor(data, weber, d0, alg_name)
	cluster.encoding()
	# f1 value, class number and cluster members
	cluster_f1s = dict()
	for i in set(tag_data):
		indexs = np.where(tag_data == i)
		cluster_f1s[i] = [0,len(indexs[0]),[],0]
	for s in range(130, 131):
		cur_code_set = cluster.currentcodes(s)
		clutter = []
		for c in cur_code_set:
			nbr_c_set = cluster.neighbors(c, cur_code_set)
			cluster.connection(c, nbr_c_set)
		rs = cluster.scale_rs(s)[0]
		for i in set(rs):
			indexs = np.where(rs == i)
			if len(indexs[0])<=1:
				continue
			max_class = 0
			max_class_num = 0
			for j in set(tag_data[indexs[0]]):
				iindexs = np.where(tag_data[indexs[0]] == j)
				if len(iindexs[0]) > max_class_num:
					max_class = j
					max_class_num = len(iindexs[0])
			r = max_class_num/(cluster_f1s[max_class][1]+0.0)
			p = max_class_num/(len(indexs[0])+0.0)
			f1 = 2*p*r/(r+p)
			if f1>cluster_f1s[max_class][0]:
				cluster_f1s[max_class][0] = f1
				cluster_f1s[max_class][3] = p
				cluster_f1s[max_class][2] = indexs[0]
	for cluster_id, (f1,temp,temp2,p) in cluster_f1s.items():
		print(cluster_id,temp,len(temp2),f1,p)
	return cluster.all_rs()
def main_face1(data, tag_data, weber, d0=0, alg_name='all_connected'):
	cluster = ce.executor(data, weber, d0, alg_name)
	cluster.encoding()
	cluster_f1s = dict()
	for i in set(tag_data):
		indexs = np.where(tag_data == i)
		cluster_f1s[i] = [0,len(indexs[0]),[]]
	for s in range(120, 125):
		cur_code_set = cluster.currentcodes(s)
		for c in cur_code_set:
			nbr_c_set = cluster.neighbors(c, cur_code_set)
			sims = []
			for ne in nbr_c_set:
				sim = cluster.executor.data[c[0]].dot(cluster.executor.data[ne])
				sim = math.e**((sim-1)/0.1)
				sims.append(sim)
			sigma = math.sqrt(np.sum(sims)/len(sims))
			nei = []
			
			new_sims = []
			for ne in nbr_c_set:
				sim = cluster.executor.data[c[0]].dot(cluster.executor.data[ne])
				sim = math.e**((sim-1)/sigma)
				new_sims.append(sim)
				if sim>=math.e**(-1.0):
					nei.append(ne)
			if len(sims)>=2:
				indexs = np.argsort(-np.array(sims))
				nei.append(nbr_c_set[indexs[1]])
			cluster.connection(c, nei)
		rs = cluster.scale_rs(s)
		for i in set(rs[0]):
			indexs = np.where(rs[0] == i)
			if len(indexs[0])<=1:
				continue
			max_class = 0
			max_class_num = 0
			for j in set(tag_data[indexs[0]]):
				iindexs = np.where(tag_data[indexs[0]] == j)
				if len(iindexs[0]) > max_class_num:
					max_class = j
					max_class_num = len(iindexs[0])
			p = max_class_num/(cluster_f1s[max_class][1]+0.0)
			r = max_class_num/len(indexs[0])
			f1 = 2*p*r/(r+p)
			if f1>cluster_f1s[max_class][0]:
				cluster_f1s[max_class][0] = f1
				cluster_f1s[max_class][2] = indexs[0]
	for cluster_id, (f1,temp,temp2) in cluster_f1s.items():
		print(cluster_id,temp,temp2,f1)
	# return cluster.all_rs()
def test_conv():
	originaldata_path = r'D:\worktemp\2019-conv\2_4.csv'
	data = np.loadtxt(originaldata_path, delimiter=',',dtype=str,skiprows=1)
	citynames = data[:,0]
	clustering_data = data[:,1:14].astype(float)

	# for i in range(len(citynames)):
	# 	for j in range(7):
	# 		clustering_data[i,j] =(clustering_data[i,j+1]-clustering_data[i,j])/(clustering_data[i,j]+1.0)
	
	for i in range(len(citynames)):
		clustering_data[i] =np.log(clustering_data[i]+1) 


	results = main(clustering_data[:,5:10], Lambda = 1)
	f=open(r'D:\worktemp\2019-conv\2_4_result.txt','w')
	for r_i,rs in enumerate(results):
		f.write('--------%d--------\r'%(r_i))
		for i in range(max(rs)):			
			indexs = np.where(rs== i)
			f.write('-----------%d-%d----------\r'%(r_i,i))
			f.write(citynames[indexs[0]])
			f.write('\r')
	f.close()

import os
import numpy as np
if __name__ == "__main__":

	# test_conv()
	originaldata_path = os.path.abspath('..') + r"\originaldata"
	outputdata_path = os.path.abspath('..') + r"\outputdata"
	data = np.load(r"D:\worktemp\clustering-python\originaldata\face_500.npy")
	tag_data = np.load(r"D:\worktemp\clustering-python\originaldata\face_500_tag.npy")
	# data = data[0:5000,:]
	# tag_data = tag_data[0:5000]
	
	results = main(data, Lambda = 0.05)
	for i, rs in enumerate(results):
		# np.savetxt(r"D:\worktemp\clustering-python\originaldata\%d.txt"%(i), rs)
		for i in set(rs[0]):
			indexs = np.where(rs[0] == i)
			if len(indexs[0])>2:
				print(tag_data[indexs[0]])
	# data = np.load(originaldata_path+'filtered_face_3W.npy')
	# tag_data = np.load(originaldata_path+'filtered_face_tag.npy')
	# data = data[0:500]
	# tag_data = tag_data[0:500]
	# data = np.load(originaldata_path+'log_5000.npy')
	# tag_data = np.load(originaldata_path+'log_label_5000.npy')
	# main_face(data, tag_data, 0.05, d0=0, alg_name='all_connected')
	# main_face(data, tag_data, 0.05, d0=0, alg_name='all_connected')