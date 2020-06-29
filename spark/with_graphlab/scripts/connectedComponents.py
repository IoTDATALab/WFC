#!/usr/bin/env python2
from graphlab import SFrame, SGraph, connected_components
import os,re

outputPath = os.environ.get("OUTPUT_PATH")
startScale = int(os.environ.get("START_SCALE"))

tagFile = './tmp'
with open(tagFile, 'r') as f:
    infor = f.readline().strip().split(",")
    maxScale = int(infor[1])
    realEndScale = int(infor[2])

scaleRange = range( startScale, realEndScale + 1 )

for scale in scaleRange:

    inputPath = os.path.join(outputPath, 'tmp', 'AdjacentRelationships', str(scale))
    url = inputPath
    data = SFrame.read_csv(url, header=False)
    if (data.num_rows() == 0):
        cc_ids = SFrame({"__id": [], "component_id": []})
    else:
        g = SGraph().add_edges(data, src_field=data.column_names()[0], dst_field=data.column_names()[1])
        cc = connected_components.create(g)
        cc_ids = cc.get('component_id')
    path = os.path.join(outputPath, 'tmp', 'ConnectedComponents', str(scale))
    if (~os.path.exists(path)):
        os.makedirs(path)

    SFrame.export_csv(cc_ids, os.path.join(path))
