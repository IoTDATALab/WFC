#!/usr/bin/env bash
source environment.sh
source parameters.sh

start=$(date +%s)

# Step1: Encoding and Neighbour Computing
export STEP=1
bash submit.sh

# Step2: GraphLab ConnectedComponents computing
#chmod +x ./connectedComponents.py
python2 ./connectedComponents.py

# Step3: Decoding
export STEP=3
bash submit.sh

end=$(date +%s)
time=$(( $end - $start ))
echo "running time: $time s"