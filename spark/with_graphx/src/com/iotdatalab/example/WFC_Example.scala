package com.iotdatalab.example

import com.iotdatalab.cluster.algorithm.WFC
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WFC_Example {

  private val defaultNumPartition = sys.env.get("SPARK_DEFAULT_PARALLELISM").get.toInt

  def main(args: Array[String]) {

    val lambda: Double = 0.02
    val dataPath = "/path/to/data/data.csv"
    val outputPath = "/path/to/save/result"

    val destOut = outputPath.split('/').last
    val conf = new SparkConf()
      .setAppName(s"Weber-Fechner clustering(lambda=$lambda) -> $destOut")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

    val sc = new SparkContext(conf)
    /**
      * Optional parameters and settings:
      *     1. startScale: Setting encoding starting scale
      *     2. endScale: Setting encoding ending scale
      *     3. codeModel: Setting encoding model. Setting "SD" for SD code, "MIN" for MinHash Code
      *     4. standardization: Standardized input data or not
      *     5. numHashTables and numBuckets: Setting MinHashLSH parameters
      */
    WFC.setMinHashLSHParas(numHashTables=180,numBuckets=18)
    WFC.setScaleRange(startScale=1,endScale=10)
    WFC.setEncodingModel(codeModel="SD")
    WFC.setDataStandardization(standardization=true)

    /**
      * Required parameters to start clustering and save results
      * 1. dataPath: Input Data Path
      * 2. outputPath: Output Results Path
      * 3. lambda: The Weber-Fechner coefficient
      */
    val data: RDD[String] = sc.textFile(dataPath, defaultNumPartition)
    WFC.cluster(data, lambda, outputPath)
    sc.stop()
  }
}