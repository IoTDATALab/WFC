import math	
import numpy as np
def get_min_dis(data):
	data_num, dim_num = data.shape
	all_mins = []
	for dim in range(0, dim_num):
		templist = np.sort(data[:, dim])
		templist = abs(templist[1:data_num] - templist[0:data_num-1])
		templist = templist[np.where(templist>0)]
		dim_min = min(templist)
		all_mins.append(dim_min)
	return min(all_mins)
def get_scale_num(weber, max, min):
	scale_num = int(math.log(max/(min+0.0), 1+weber))
	return scale_num