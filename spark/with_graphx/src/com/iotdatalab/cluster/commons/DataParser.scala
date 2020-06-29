package com.iotdatalab.cluster.commons

import breeze.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.math.{floor, log, pow}

class DataParser extends Serializable {
  private final val logger = LoggerFactory.getLogger(this.getClass)
  // Standardized vector value of data. Each value of the vector should be divided by the norm of this vector.
  private def standardData(data: RDD[Array[Double]]): RDD[Array[Double]] = {
    data.map(arr => {
      val vectorNorm: Double = norm(DenseVector(arr))
      arr.map(_ / vectorNorm)
    })
  }
  // Computing the precision of input data set. Precision: max effective decimal places for each dimension of data
  private def calDataPrecisionPerDim(data: RDD[String]): Array[Int] = {
    val dataPrecision: RDD[Array[Double]] = data.map(_.split("\\s+|,").map(str => str.split('.')(1).length.toDouble))
    val summary: MultivariateStatisticalSummary = Statistics.colStats(dataPrecision.map(Vectors.dense(_)))
    val pecisionPerDim: Array[Double] = summary.max.toArray
    pecisionPerDim.map(_.toInt)
  }
  // Computing the max coding scale
  // The lowest precision of all data dimensions determines the max coding scale in this algorithm.
  private def calMaxSDCodeScale(interval: Array[Double] , dataPrecision: Array[Int]) = {
    val enlargedInternal = interval.zip(dataPrecision).map{ case (int, pre) => int*pow(10, pre)}
    val maxScale = enlargedInternal.map(log(_) / log(2)).map(floor(_)).min.toInt
    maxScale - 1
  }
  // Computing the values interval for every dimensions.
  private def intervalStatistics(parsedData: RDD[Vector]): Array[Double] = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(parsedData)
    val lowerBound = summary.min.toArray
    val upperBound = summary.max.toArray
    val interval = lowerBound.zip(upperBound).map { case (low, up) => up - low }
    interval
  }
  //Calculating the maximum scale of Jaccard similarity in LSH when neighbour computing
  private def calMaxJaccardScale(dimensions: Int, lambda: Double) = floor(log(dimensions) / log(1 + lambda)).toInt - 1
  // Comparing input ending scale with maximum scale that we calculate according to lambda and data dimensions
  private def updateEndScale(endScale: Int, maxScale: Int): Int = {
    var realEndScale = 0
    if(endScale == 0) {
      realEndScale = maxScale
    } else if (endScale > maxScale) {
      logger.info(s"EndScale ${endScale} can not be greater maxScale ${maxScale}.")
      realEndScale = maxScale
    } else if (endScale <= maxScale) {
      realEndScale = endScale
    }
    realEndScale
  }

  /**
    * Parsing data
    * @param data original data
    * @param lambda The Weber-Fechner coefficient
    * @return (vector transformed form original data, data dimensions, maximum encoding scale)
    */
  def parse(data: RDD[String],
            lambda: Double,
            codeModel: String,
            endScale: Int,
            standardization: Boolean = true): (RDD[Vector], Int, Int) = {
    val parsedData: RDD[Array[Double]] = data.map(_.split("\\s+|,").map(_.toDouble))
    val dimensions = parsedData.first().length
    if (dimensions == 0) {
      throw new IllegalArgumentException("The dimension of each item must be more than 1. " +
        "Please check the first item again!")
    }
    val vectorsRDD: RDD[Vector] = if (standardization == true) {
      standardData(parsedData).mapPartitions(iterator => iterator.map(Vectors.dense(_)))
    } else {
      parsedData.mapPartitions(iterator => iterator.map(Vectors.dense(_)))
    }
    val maxScale: Int = if (codeModel == "SD") {
      val dataPrecision: Array[Int] = calDataPrecisionPerDim(data)
      val interval = intervalStatistics(vectorsRDD)
      calMaxSDCodeScale(interval, dataPrecision)
    } else calMaxJaccardScale(dimensions, lambda)

    // Update real ending Weber scale
    val realEndScale = updateEndScale(endScale,maxScale)

    (vectorsRDD, dimensions, realEndScale)
  }
}
