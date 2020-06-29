package com.iotdatalab.cluster.commons

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.math.{floor, pow}
import scala.util.Random

class CodingFunctions extends Serializable {

  //////////////////////////////////////////////////////
  // Coding functions for SD Model
  // Converting Vector data to SD code
  //////////////////////////////////////////////////////

  // computing the min, max value and their difference for every dimensions.
  private def intervalStatistics(parsedData: RDD[Vector]): Array[(Double, Double, Double)] = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(parsedData)
    val lowerBound = summary.min.toArray
    val upperBound = summary.max.toArray
    val difference = lowerBound.zip(upperBound).map { case (low, up) => up - low }
    lowerBound.zip(upperBound).zip(difference).map { case ((low, up), diff) => (low, up, diff) }
  }
  // Coding data to SD code in max scale
  def dataToSDCodeInMaxScale(
                              parsedData: RDD[Vector],
                              statistics: Array[(Double, Double, Double)],
                              dimensions: Int,
                              maxScale: Int): RDD[(Int, String)] = {
    val sc = parsedData.context
    val bcStats = sc.broadcast(statistics)
    val bcScale = sc.broadcast(maxScale)
    val bcDimens = sc.broadcast(dimensions)
    val formatString = s"%0${maxScale}d"
    val bcFormatString = sc.broadcast(formatString)
    val codes = parsedData.zipWithIndex()
      .mapPartitions(iterator => iterator.map { case (data, id) => {
        val value = data.toArray
        val projected: Array[Double] = value.zip(bcStats.value).map(r => {
          if (r._1 == r._2._1) 0
          else if (r._1 == r._2._2) (pow(2, bcScale.value) - 1)
          else floor((r._1 - r._2._1) / r._2._3 * pow(2, bcScale.value))
        })
        val temp: Array[String] = projected
          .map(x => bcFormatString.value.format(BigInt(BigInt(x.toLong).toString(2))))
        val code: String = (0 until bcScale.value).map(i => {
          var j = 0
          val str = new StringBuilder
          while (j < bcDimens.value) {
            str.append(temp(j).charAt(i))
            j += 1
          }
          str.toString()
        }).mkString("")
        (id.toInt, code)
      }})
    codes
  }
  // Intercept max-scale codes for codes in other scales.
  private def interceptCodesWithScale(maxScaleCodes: RDD[(Int, String)], scale: Int, dimensions: Int) = {
    val sc = maxScaleCodes.context
    val length = scale * dimensions
    val bcLength = sc.broadcast(length)
    val scaleResult: RDD[(Int, String)] = maxScaleCodes
      .mapPartitions(iterator => iterator.map { case (id, code) =>
        (id, code.take(bcLength.value))
      })
    scaleResult
  }
  // Calculating distinct codes and their points ids.
  private def distinctCodesAndIdsWithScale(idAndCodeWithScale: RDD[(Int, String)]) = {
    val numPartitions = idAndCodeWithScale.getNumPartitions
    val distinctCodesAndIds: RDD[(String, mutable.Iterable[Int])] = idAndCodeWithScale.map(_.swap)
      .aggregateByKey(mutable.Iterable.empty[Int],numPartitions)((set, id) =>
        set ++ mutable.Iterable(id), (set1, set2) => set1 ++ set2)
    distinctCodesAndIds
  }
  // Calculating distinct codes and their points ids.
  def distinctCodesAndPointsNumWithScale(idAndCode: RDD[(Int, String)]) = {
    val distinctCodesAndPointsNum = idAndCode
      .mapPartitions(iterator => iterator.map { case (id, code) => (code, 1)})
      .reduceByKey(_ + _)
    distinctCodesAndPointsNum
  }

  ///////////////////////////////////////////////////////
  // Coding functions for MinHashLSH Model
  // Converting Vector data to MinHash code
  ///////////////////////////////////////////////////////

  private val PRIME = 2147483647
  // Converting data to string shingles.
  private def dataToShingle(data: RDD[Vector]): RDD[(Int, Array[String])] = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(data)
    val (lowerBound, upperBound) = (summary.min.toArray, summary.max.toArray)
    val mid = lowerBound.zip(upperBound).map { case (min, max) => min + (max - min) / 2 }
    val bcMid = data.context.broadcast(mid)
    val idAndSingle = data.zipWithIndex().map { case (feature, id) => {
      val shingle = feature.toArray.zip(bcMid.value)
        .zipWithIndex.map { case ((x, mid), index) => if (x >= mid) s"$index-1" else s"$index-0"}
      (id.toInt, shingle)}
    }
    idAndSingle
  }
  // Converting feature vectors to minHash signatures(codes).
  private def minHash(features: Array[String], hashFunctions: List[Int => Int]): String = {

    var index = 0
    val result = new mutable.ArrayBuffer[Int]
    while (index < hashFunctions.length) {
      var min = Int.MaxValue
      for (f <- features) {
        val hashResult = hashFunctions(index).apply(f.hashCode)
        if (hashResult < min) min = hashResult
      }
      result.append(min)
      index += 1
    }
    result.toArray.mkString(",")
  }
  // Generating hash functions for MinHash.
  private def getHashFunctions(number: Int, seed: Int = 100 ) : List[Int => Int] = {
    val result = new mutable.ArrayBuffer[Int => Int]()
    val rand = new Random(seed)
    var index = 0
    while (index < number) {
      val a = rand.nextInt()
      val b = rand.nextInt()
      result.append((x) => ((a.toLong * x + b) % PRIME).toInt)
      index += 1
    }
    result.toList
  }
  // Converting shingle data to minHash signatures.
  private def shingleToMinHashSignature(data: RDD[(Int, Array[String])], numHashTables: Int): RDD[(Int, String)] = {
    val bcHashFunctions = data.context.broadcast(getHashFunctions(numHashTables))
    val minHashRDD = data.map{case (id, features) =>
      (id, minHash(features, bcHashFunctions.value))}
    minHashRDD
  }


  /**
    * Encoding data in maximum scale
    * It is for two model --Low Dimensional Model(LDM) and High Dimensional Model(HDM)
    * @param parsedData Vector data
    * @param dimensions Data dimensions
    * @param codeModel Coding model
    * @param maxScale Maximum Weber scale
    * @param numHashTables For MIN coding model, it is a parameter for MinHash Signature calculation
    * @return (Item ID, code) in maximum Weber scale
    */
  def encodeDataInMaxScale(parsedData: RDD[Vector], dimensions: Int, codeModel: String,
                           maxScale: Int, numHashTables: Int): RDD[(Int, String)] = {
    if (codeModel == "SD") {
      val statistics: Array[(Double, Double, Double)] = intervalStatistics(parsedData)
      val idAndCode = dataToSDCodeInMaxScale(parsedData, statistics, dimensions, maxScale)
      idAndCode
    }
    else {
      val shingle = dataToShingle(parsedData)
      val idAndCode = shingleToMinHashSignature(shingle, numHashTables)
      idAndCode
    }
  }

  /**
    * Getting codes in specified coding scale
    * @param idAndCodeInMaxScale Pairs of item ID and its code in maximum Weber scale
    * @param dimensions Data dimensions
    * @param codeModel Coding model
    * @param scale Weber scale
    * @param maxEncodeScale maximum encoding scale
    * @param outputPath Output Files Path
    * @return (item ID, code) in this "scale"
    */
  def getDistinctCodesWithScale(
                        idAndCodeInMaxScale: RDD[(Int, String)],
                        dimensions: Int,
                        codeModel: String,
                        scale: Int,
                        maxEncodeScale: Int,
                        outputPath: String): RDD[(Int, String)] = {
    //Temporary File Path
    val tmpFilePath = s"${outputPath}/tmp"
    if (codeModel == "SD") {
      val idAndCodeWithScale =
        if (scale < maxEncodeScale)
          interceptCodesWithScale(idAndCodeInMaxScale, scale, dimensions)
        else
          idAndCodeInMaxScale
      idAndCodeWithScale.saveAsObjectFile(s"${tmpFilePath}/IdAndCode/${scale}")
      val codeAndPointsNumThisScale: RDD[(String, Int)] = distinctCodesAndPointsNumWithScale(idAndCodeWithScale)
      val distinctCodesThisScale = codeAndPointsNumThisScale
        .mapPartitions(iterator => iterator.map { case (code, num) => (0, code)})
      distinctCodesThisScale
    }
    else {
      idAndCodeInMaxScale.mapPartitions(iterator => iterator.map { case (id, code) => (id.toString, id.toString)})
        .saveAsObjectFile(s"${tmpFilePath}/IdAndCode/${scale}")
      idAndCodeInMaxScale
    }
  }

  /**
    * Decoding data
    * @param codeAndCluster Code and label pairs from Connected Component
    * @param dimensions Data dimensions
    * @param scale Weber Scale
    * @param codeModel Coding model
    * @param outputPath Output File Path
    * @return (item ID, Cluster ID) in this "scale"
    */
  def decodeResults(codeAndCluster: RDD[(String, String)],
                    dimensions: Int,
                    scale: Int,
                    codeModel: String,
                    outputPath: String): RDD[(Int, String)] = {
    val sc = codeAndCluster.context
    // Temporary File Path
    val tmpFilePath = s"${outputPath}/tmp"
    // File path of pairs of item ID and code in specific scale
    val idAndCodePath = s"${tmpFilePath}/IdAndCode/${scale}"

    val length = dimensions * scale
    val formatString = s"%0${length}d"
    val bcFormatString = sc.broadcast(formatString)

    if (codeModel == "SD") {
      val idAndCodeWithScale: RDD[(Int, String)] = sc.objectFile[(Int, String)](idAndCodePath)

      val ccResult = codeAndCluster.mapPartitions(iterator => iterator.map {
        case (code, cluster) => (bcFormatString.value.format((BigInt(code.toLong.toBinaryString))), cluster)
      })
      val codeAndTmpCluster = distinctCodesAndPointsNumWithScale(idAndCodeWithScale).mapPartitions(iterator => iterator.map{
        case (code, _) => (code, (-BigInt(code,2)).toString())
      })
      val allCodeAndCluster: RDD[(String, String)] = codeAndTmpCluster.subtractByKey(ccResult).union(ccResult)
      val idAndCluster: RDD[(Int, String)] = idAndCodeWithScale.mapPartitions(iter => iter.map(_.swap))
        .join(allCodeAndCluster).map(_._2)

      idAndCluster
    }
    else {
      codeAndCluster.mapPartitions(iterator => iterator.map { case (id, cluster) => (id.toLong.toInt, cluster) })
    }
  }

}
