#!/usr/bin/env bash
source environment.sh

spark-submit --class com.iotdatalab.example.WFC_Example \
             --master ${SPARK_MASTER} \
             --conf spark.dirver.memory=${SPARK_DRIVER_MEMORY} \
             --conf spark.driver.maxResultSize=${SPARK_DRIVER_MAXRESULTSIZE} \
             --conf spark.kryoserializer.buffer.max=${SPARK_KRYOSERIALIZER_MAXRESULTSIZE} \
             --conf spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM} \
             --num-executors ${SPARK_NUM_EXECUTORS} \
             --executor-memory ${SPARK_EXECUTORS_MEMORY} \
             --executor-cores ${SPARK_EXECUTORS_CORES} \
             WFC-Spark.jar
