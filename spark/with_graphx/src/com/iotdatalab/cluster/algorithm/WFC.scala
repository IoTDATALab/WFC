package com.iotdatalab.cluster.algorithm

import java.io._

import com.iotdatalab.cluster.commons.{CodingFunctions, ConnectedComponents, DataParser, NeighbourComputer}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.LoggerFactory

object WFC {

  // Initialization optional parameters
  private var NUMBER_OF_HASHTABLES: Int = 125
  private var NUMBER_OF_BUCKETS: Int = 25
  private var START_SCALE: Int = 1
  private var END_SCALE: Int = 0
  private var CODE_MODEL: String = "SD"
  private var STANDARDIZATION: Boolean = true

  /**
    * MinHashLSH parameters setter
    * @param numHashTables The length of the MinHash signature for every item.
    * @param numBuckets The number of bands in LSH for every item. The numHashTables must be divisible by numBuckets
    */
  def setMinHashLSHParas(numHashTables: Int, numBuckets: Int) = {
    if (NUMBER_OF_HASHTABLES % NUMBER_OF_BUCKETS != 0) {
      throw new IllegalArgumentException("The \"numHashTables\" must be divisible by \"numBuckets\" ! ")
    }
    NUMBER_OF_HASHTABLES = numHashTables
    NUMBER_OF_BUCKETS = numBuckets
  }

  /**
    * Weber scale range Setter
    * @param startScale The starting Weber scale.
    *                   The startScale must be greater than or equal to 1.
    * @param endScale The ending Weber scale.
    *                 The endScale should be greater than or equal to startScale.
    *                 But if endScale is 0, clustering will run from the startScale to the maximum Weber scale .
    */
  def setScaleRange(startScale: Int, endScale: Int): Unit = {
    if (START_SCALE < 1 || END_SCALE < 0) {
      throw new IllegalArgumentException("The endScale must be greater than or equal to 0," +
        "and startScale must be greater than or equal to 1.")
    }
    if (END_SCALE != 0 && END_SCALE < START_SCALE) {
      throw new IllegalArgumentException("The endScale must be greater than " +
        "or equal to startScale.")
    }
    START_SCALE = startScale
    END_SCALE = endScale
  }

  /**
    * Encoding model setter
    * @param codeModel The encoding model, "SD" for SD Code, and "MIN" for MinHash Code
    * @return Unit
    */
  def setEncodingModel(codeModel: String) = try {
    CODE_MODEL = codeModel
  } catch {
    case ex: IllegalArgumentException => {
      println("The \"codeModel\" should be set a string that either SD or MIN. " +
        "SD for SD Code, and MIN for MinHash Code")
    }
  }

  /**
    * Data standardization setter
    * @param standardization Standardized input data or not
    * @return Unit
    */
  def setDataStandardization(standardization: Boolean) = {
    STANDARDIZATION = standardization
  }

  /**
    * To start Weber–Fechner Clustering
    * @param data RDD[String],The input data that must be numerical data vector and be separated by commas or any white space.
    *             It is a string rdd here.
    * @param lambda The Weber-Fechner coefficient
    * @param outputPath The output path for clustering results and temporary results
    * @return WFC object
    */
  def cluster( data: RDD[String], lambda: Double, outputPath: String): WFC = {
    new WFC(lambda).cluster(data, outputPath)
  }
}

class WFC private(val lambda: Double) extends Serializable {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  //Delete temporary files
  private def deleteTempFile(sc: SparkContext, outputPath:String) = {
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val tmpFilePath = new Path(s"${outputPath}/tmp")
    if (hdfs.exists(tmpFilePath))
      hdfs.delete(tmpFilePath, true)
    // Deleting local temporary files
    val path: File = new File("./tmp")
    if (path.exists()) path.delete()

  }

  // Weber-Fechner clustering
  private def cluster(data: RDD[String], outputPath: String): WFC = {
    // New function objects
    val (dp, cf, nc, cc) = (new DataParser, new CodingFunctions, new NeighbourComputer, new ConnectedComponents)
    // Setting checkpoint
    data.context.setCheckpointDir(s"${outputPath}/tmp/checkpoint")
    // Parsing data for information and vector data.
    val (parsedData, dimensions, realEndScale) = dp.parse(data, lambda, WFC.CODE_MODEL, WFC.END_SCALE, WFC.STANDARDIZATION)
    parsedData.cache()
    // Encoding data in maximum Weber scale
    val idAndCodes = cf.encodeDataInMaxScale(parsedData, dimensions, WFC.CODE_MODEL, realEndScale, WFC.NUMBER_OF_HASHTABLES)
    // Loop in scale range
    for (scale <- WFC.START_SCALE to realEndScale) {
      //Getting non-repeating code in specific weber scale
      val distinctCodeThisScale = cf.getDistinctCodesWithScale(idAndCodes, dimensions, WFC.CODE_MODEL, scale, realEndScale, outputPath)
      //Neighbour computing to get similar code pairs(relationships)
      val codeRelationPairs: RDD[(String, String)] = nc.neighbourComputing(
        distinctCodeThisScale, dimensions, WFC.CODE_MODEL, scale, lambda, WFC.NUMBER_OF_BUCKETS)
      //Connected Components
      val codeAndCluster: RDD[(String, String)] = cc.connectedComponents(codeRelationPairs, dimensions, WFC.CODE_MODEL, scale)
      // Decoding for (item ID, cluster ID)
      val itemIdAndCluster: RDD[(Int, String)] = cf.decodeResults(codeAndCluster, dimensions, scale, WFC.CODE_MODEL, outputPath)
      // Sorting and saving clustering results
      itemIdAndCluster.repartitionAndSortWithinPartitions(new HashPartitioner(1))
        .map { case (id, cluster) => id + "," + cluster }
        .saveAsTextFile(s"${outputPath}/idAndCluster/${scale}")
    }
    parsedData.unpersist()
    // Deleting temporary files
    deleteTempFile(data.context, outputPath)
    new WFC(lambda)
  }
}