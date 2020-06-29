import numpy as np
import math
import networkx as nx


def sdcoding_cal_dim_para(input_data, dim_num, scale_num, is_equal):
    dim_para = np.zeros((dim_num, 3), np.float)
    for d_i in range(0, dim_num):
        d_max = np.max(input_data[:, d_i])
        d_min = np.min(input_data[:, d_i])
        d_gap = (d_max - d_min) / ((2 ** scale_num) + 0.0)
        dim_para[d_i, :] = [d_max, d_min, d_gap]
    if is_equal:
        gap = max(dim_para[:, 2])
        dim_para[:, 0] = dim_para[:, 1] + gap * (2 ** scale_num)
        dim_para[:, 2] = gap
    return dim_para


def sdcoding_pro_encode(input_data, dim_num, scale_num, dim_para, indexs):
    code_map = []
    format_str = '{0:0' + str(scale_num) + 'b}'
    row, col = input_data.shape
    for data_i, data in enumerate(input_data):
        sdcode = list('0' * scale_num * dim_num)
        for d_i in range(0, dim_num):
            r_grid = (data[d_i] - dim_para[d_i, 1]) / dim_para[d_i, 2]
            if r_grid >= 2 ** scale_num:
                r_grid = 2 ** scale_num - 1;
            if r_grid < 0:
                r_grid = 0;
            r_grid = int(r_grid);
            r_str = format_str.format(r_grid)
            for s_i in range(0, scale_num):
                sdcode[d_i + s_i * dim_num] = r_str[s_i]
        code_map.append(sdcode)
    return code_map


def sdcoding_nei_find(code, dim_num, neighbour_num, select_scale):
    format_str = '{0:0' + str(select_scale) + 'b}'
    code_list = list(code)
    r_grid = np.zeros((dim_num, 1), np.int)
    r_pool = [];
    for d_i in range(0, dim_num):
        r_grid[d_i] = int(''.join(code_list[d_i:dim_num * select_scale:dim_num]), 2)
        r_pool.append([])
        r_pool[d_i].extend(r_grid[d_i])
    for n_i in range(1, neighbour_num + 1):
        r_max = r_grid + n_i;
        r_min = r_grid - n_i;
        for d_i in range(0, dim_num):
            if (r_max[d_i] < 2 ** select_scale):
                r_pool[d_i].extend(r_max[d_i])
            if (r_min[d_i] >= 0):
                r_pool[d_i].extend(r_min[d_i])
    nei_set = set()
    for i in range(0, dim_num):
        if nei_set:
            new_set = set()
            for code in nei_set:
                for r_i in r_pool[i]:
                    nei_code = list(code)
                    r_ii = format_str.format(r_i)
                    nei_code[i:select_scale * dim_num:dim_num] = r_ii
                    new_set.add(''.join(nei_code))
            nei_set = new_set
        else:
            for r_0 in r_pool[0]:
                nei_code = list('0' * select_scale * dim_num)
                r_00 = format_str.format(r_0)
                nei_code[0:select_scale * dim_num:dim_num] = r_00
                nei_set.add(''.join(nei_code))
    return nei_set


def sdcoding_code_distance(code, nei_code, dim_num):
    code_list = list(code)
    nei_list = list(nei_code)
    r_code = np.zeros((dim_num, 1), np.int)
    r_nei = np.zeros((dim_num, 1), np.int)
    r_dis = np.zeros((dim_num, 1), np.int)
    for d_i in range(0, dim_num):
        r_code[d_i] = int(''.join(code_list[d_i:len(code_list):dim_num]), 2)
        r_nei[d_i] = int(''.join(nei_list[d_i:len(nei_list):dim_num]), 2)
        r_dis[d_i] = np.abs(r_code[d_i] - r_nei[d_i])
    dis = np.max(r_dis)
    return dis


def get_scale_num(originaldata, scale_lambda):
    data_num, dim_num = originaldata.shape
    scales = []
    for dim in range(0, dim_num):
        templist = np.sort(originaldata[:, dim])
        templist = abs(templist[1:data_num] - templist[0:data_num - 1])
        templist = templist[np.where(templist > 0)]
        delta = min(templist) * scale_lambda
        max_value = max(originaldata[:, dim])
        min_value = min(originaldata[:, dim])
        scale = math.floor(math.log(abs(max_value - min_value) / delta, 2))
        scales.append(scale)
    return int(max(scales))


def scale_para(scale_num, scale_lambda):
    scale_ = int(scale_num * math.log(2, 1 + scale_lambda))
    list = [int((1 + scale_lambda) ** a) for a in range(0, scale_)]
    return set(list)


def encoding_sdcode(inputdata, scale_num):
    data_num, dim_num = inputdata.shape
    dim_para = sdcoding_cal_dim_para(inputdata, dim_num, scale_num, True)
    sdcodes = sdcoding_pro_encode(inputdata, dim_num, scale_num, dim_para, [])
    return np.array(sdcodes)


def sdcode_neighbor_build(sdcodes, scale, dim_num):
    data_num, bit_len = sdcodes.shape
    scale_sdcodes = sdcodes[:, 0:bit_len - scale * dim_num]
    scale_dict = dict()
    for i, item in enumerate(scale_sdcodes):
        key = ''.join(item)
        if scale_dict.has_key(key):
            templist = scale_dict[key]
            templist.append(i)
            scale_dict[key] = templist
        else:
            templist = []
            templist.append(i)
            scale_dict[key] = templist
    scale_codes = scale_dict.keys()
    return scale_codes, scale_dict


def get_kernel_map(dim_num, scale, code_map, alg_name):
    attr_map = dict()
    kernel_map = dict()
    for code, points in code_map.items():
        nei_codes = sdcoding_nei_find(code, dim_num, 1, scale)
        kernel = 0
        for nei_code in nei_codes:
            if code_map.has_key(nei_code):
                kernel += len(code_map[nei_code])
        kernel_map[code] = (kernel, points)
    return kernel_map


def get_maxkernel_code(neighbor_map, code, nei_codes):
    max_code = code
    max_kernel = neighbor_map[code][0]
    for nei_code in nei_codes:
        if neighbor_map.has_key(nei_code):
            if max_kernel <= neighbor_map[nei_code][0]:
                max_code = nei_code
                max_kernel = neighbor_map[nei_code][0]
    return max_code
