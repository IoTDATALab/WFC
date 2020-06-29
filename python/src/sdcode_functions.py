import numpy as np
import clustering_functions as cf
def encoding(data, delta, is_equal):
	data_num, dim_num = data.shape
	scale_num = 1
	if delta !=-1:
		scale_num = cf.get_scale_num(1, np.max(data) - np.min(data), delta)
	dim_para = sdcoding_cal_dim_para(data, dim_num, scale_num, is_equal)
	codes = sdcoding_pro_encode(data, dim_num, scale_num, dim_para,[])
	return codes

def sdcoding_cal_dim_para(input_data, dim_num, scale_num, is_equal):
	dim_para = np.zeros((dim_num,3),np.float)
	for d_i in range(0,dim_num):
		d_max = np.max(input_data[:,d_i])
		d_min = np.min(input_data[:,d_i])
		d_gap = (d_max-d_min)/((2**scale_num)+0.0)
		dim_para[d_i,:]=[d_max, d_min, d_gap]
	if is_equal:
		gap = max(dim_para[:,2])
		dim_para[:,0] = dim_para[:,1]+gap*(2**scale_num)
		dim_para[:,2] = gap
	return dim_para
def sdcoding_pro_encode(input_data, dim_num, scale_num, dim_para,indexs):
	code_map=[]
	format_str='{0:0'+str(scale_num)+'b}'
	row,col = input_data.shape
	for data_i,data in enumerate(input_data):
		sdcode = list('0'*scale_num*dim_num)
		for d_i in range(0, dim_num):
			r_grid = 0
			if dim_para[d_i,2]!=0:
				r_grid = (data[d_i]-dim_para[d_i,1])/dim_para[d_i,2]
				if r_grid >= 2**scale_num:
					r_grid=2**scale_num-1;
				if r_grid < 0:
					r_grid= 0;
				r_grid=int(r_grid);
			r_str=format_str.format(r_grid)
			for s_i in range(0, scale_num):
				sdcode[d_i+s_i*dim_num]=r_str[s_i] 
		code_map.append(sdcode)
	return code_map

def neighbors(code, dim_num, neighbour_num):
	select_scale = len(code)/dim_num
	format_str='{0:0'+str(select_scale)+'b}'
	code_list=list(code)
	r_grid=np.zeros((dim_num,1),np.int)
	r_pool=[];
	for d_i in range(0,dim_num):
		r_grid[d_i]=int(''.join(code_list[d_i:int(dim_num*select_scale):dim_num]),2)
		r_pool.append([])
		r_pool[d_i].extend(r_grid[d_i])
	for n_i in range(1, neighbour_num+1):
		r_max = r_grid+n_i;
		r_min = r_grid-n_i;
		for d_i in range(0, dim_num):
			if(r_max[d_i]<2**select_scale):
				r_pool[d_i].extend(r_max[d_i])
			if(r_min[d_i]>=0):
				r_pool[d_i].extend(r_min[d_i])
	nei_set = set()
	for i in range(0,dim_num):
		if nei_set:
			new_set = set()
			for code in nei_set:
				for r_i in r_pool[i]:
					nei_code=list(code)
					r_ii = format_str.format(r_i)
					nei_code[i:select_scale*dim_num:dim_num]=r_ii
					new_set.add(''.join(nei_code))
			nei_set = new_set
		else:
			for r_0 in r_pool[0]:
				nei_code = list('0'*int(select_scale*dim_num))
				r_00 = format_str.format(r_0)
				nei_code[0:select_scale*dim_num:dim_num]=r_00
				nei_set.add(''.join(nei_code))
	return nei_set