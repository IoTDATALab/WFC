package com.iotdatalab.run

import com.iotdatalab.cluster.algorithm.WFC
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WFC_Job {

  private val defaultNumPartition = sys.env.get("SPARK_DEFAULT_PARALLELISM").get.toInt
  private val dataPath: String = sys.env.get("DATA_PATH").get
  private val outputPath: String = sys.env.get("OUTPUT_PATH").get
  private val codeModel: String = sys.env.get("CODE_MODEL").get
  private val lambda: Double = sys.env.get("LAMBDA").get.toDouble
  private val startScale: Int = sys.env.get("START_SCALE").get.toInt
  private val endScale: Int = sys.env.get("END_SCALE").get.toInt
  private val numHashTables: Int = sys.env.get("NUMBER_OF_HASHTABLES").get.toInt
  private val numBuckets: Int = sys.env.get("NUMBER_OF_BUCKETS").get.toInt
  private val standardization: String = sys.env.get("DATA_STANDARDIZATION").get
  private val stepTag:String = sys.env.get("STEP").get

  def main(args: Array[String]) {
    val destOut = outputPath.split('/').last
    val conf = new SparkConf()
      .setAppName(s"Weber-Fechner clustering-Part${stepTag}(lambda=$lambda) -> $destOut")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
      .set("spark.shuffle.file.buffer", "64k")
      .set("spark.reducer.maxSizeInFlight", "96m")
    val sc = new SparkContext(conf)

    val data: RDD[String] = sc.textFile(dataPath, defaultNumPartition)
    //Optional Settings
    WFC.setMinHashLSHParas(numHashTables,numBuckets)
    WFC.setScaleRange(startScale,endScale)
    WFC.setEncodingModel(codeModel)
    WFC.setDataStandardization(standardization)
    //Clustering and outputting results
    WFC.cluster(data, lambda, outputPath, stepTag)
    sc.stop()
  }
}