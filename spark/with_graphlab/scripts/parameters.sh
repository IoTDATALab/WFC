#!/usr/bin/env bash
# Optional parameters
# 1. START_SCALE: Setting encoding starting scale
# 2. END_SCALE: Setting encoding ending scale
# 3. CODE_MODEL: Setting encoding model. Setting SD for SD code, MIN for MinHash Code
# 4. STANDARDIZATION: Standardized input data or not
# 5. NUMBER_OF_HASHTABLES and NUMBER_OF_BUCKETS: Setting MinHashLSH parameters
export START_SCALE=1
export END_SCALE=0
export CODE_MODEL=SD
export DATA_STANDARDIZATION=TRUE
export NUMBER_OF_HASHTABLES=125
export NUMBER_OF_BUCKETS=25

# Required parameters:
# 1. DATA_PATH: Input Data Path
# 2. OUTPUT_DIR: Output Results Path
# 3. LAMBDA: The Weber-Fechner coefficient
export DATA_PATH=${HDFS_ADDRESS}/path/to/data
export OUTPUT_PATH=${HDFS_ADDRESS}/path/to/save/results
export LAMBDA=0.02


