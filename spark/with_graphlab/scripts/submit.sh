#!/usr/bin/env bash
source environment.sh
source parameters.sh

spark-submit --class com.iotdatalab.run.WFC_Job \
             --master ${SPARK_MASTER} \
             --conf spark.dirver.memory=${SPARK_DRIVER_MEMORY} \
             --conf spark.driver.maxResultSize=${SPARK_DRIVER_MAXRESULTSIZE} \
             --conf spark.kryoserializer.buffer.max=${SPARK_KRYOSERIALIZER_MAXRESULTSIZE} \
             --conf spark.memory.fraction=${SPARK_MEMORY_FRACTION} \
             --conf spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM} \
             --num-executors ${SPARK_NUM_EXECUTORS} \
             --executor-memory ${SPARK_EXECUTORS_MEMORY} \
             --executor-cores ${SPARK_EXECUTORS_CORES} \
             WFC-GraphLab.jar