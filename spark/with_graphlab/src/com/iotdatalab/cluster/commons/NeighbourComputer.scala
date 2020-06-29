package com.iotdatalab.cluster.commons

import org.apache.spark.rdd.RDD
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.pow

class NeighbourComputer extends Serializable {

  ///////////////////////////////////////
  // Neighbour computing for SD Model
  // Converting Vector data to SD code.
  ///////////////////////////////////////

  // Calculating pairs of distinct codes with their neighbours
  private def calNeiCodePairsWithScale(distinctCodes: RDD[String], scale: Int, dimensions: Int) = {
    val sc = distinctCodes.context
    val bcDimensions = sc.broadcast(dimensions)
    var neiCodePairsWithScale = sc.makeRDD(Seq[(String, String)]())
    if (distinctCodes.count() > 0) {
      val bf = computeCodeBloomFilter(distinctCodes)
      val bcBF = sc.broadcast(bf)
      val bcScale = sc.broadcast(scale)
      val full = BigInt("1" * scale * dimensions, 2)
      val lower = BigInt("1" * dimensions, 2)
      val templates: Array[Array[BigInt]] = (0 until dimensions).map(dimen => {
        val format_str = s"%0${dimensions}d"
        val normalTemplate =
          BigInt(format_str.format(BigInt((BigInt("1", 2) << dimen).toString(2))) * scale, 2)
        val negativeTemplate =
          BigInt(format_str.format(BigInt((BigInt("1", 2) << dimen).toString(2))), 2)
        val positiveTemplate = ((~normalTemplate) & full) | (normalTemplate & lower)
        Array(normalTemplate, negativeTemplate, positiveTemplate)
      }).toArray
      val bcTemplates = sc.broadcast(templates)
      val adjacentCodes: RDD[(String, String)] = distinctCodes.mapPartitions(iterator => {
        val bf = bcBF.value
        val scale = bcScale.value
        val dimens = bcDimensions.value
        val length = scale * dimens
        val format_str = s"%0${length}d"
        iterator.flatMap(code => {
          val tempCode = BigInt(code, 2)
          val dimenCodes: Array[Array[BigInt]] = bcTemplates.value.map(template => {
            val array = new ArrayBuffer[BigInt]()
            val data = tempCode & template(0)
            array += data
            if (data >= template(1)) {
              val negativeDataTemp = (data - template(1)) & template(0)
              array += negativeDataTemp
            }
            val positiveDataTemp = data + template(2)
            if ((positiveDataTemp >> length) == 0) array += (positiveDataTemp & template(0))
            array.toArray
          })
          val candidates: Array[String] = dimenCodes
            .reduce((a, b) => a.flatMap((x => b.map(y => (x | y)))))
            .map(x => format_str.format(BigInt(x.toString(2))))

          val filtered = candidates.filter(x => bf.mightContain(x))
          filtered.map(x => (code, x))
        })
      })
      neiCodePairsWithScale = adjacentCodes
    }
    neiCodePairsWithScale
  }
  // Generating a bloom filter using codes
  private def computeCodeBloomFilter(codes: RDD[String]): BloomFilter = {
    val count = codes.count
    val sc = codes.sparkContext
    val bf = BloomFilter.create(count, 0.00000001f)
    val bcBF = sc.broadcast(bf)
    val result = codes.mapPartitions(iterator => {
      val bf = bcBF.value
      while (iterator.hasNext) {
        bf.putString(iterator.next())
      }
      Iterator(bf)
    }).reduce(_ mergeInPlace _)
    result
  }

  ///////////////////////////////////////////////
  // Neighbour computing for MinHashLSH Model
  // Converting Vector data to MinHash code
  ///////////////////////////////////////////////

  // Calculating minHash similarity between two items.
  private def minHashSimilarity[A](item1: Array[A], item2: Array[A]): Double = {
    if (item1.length != item2.length) {
      throw new IllegalArgumentException("MinHashes must be equal length")
    }
    val agreeingRows = item1.zip(item2).map { case (val1, val2) => if (val1 == val2) 1 else 0}.sum
    agreeingRows.toDouble / item1.length.toDouble
  }
  // The similarity between each two items must be greater than this value.
  private def calJaccardSimilarityLowerLimit(lambda: Double, scale: Double, dimensions: Int) = {
    //    Sim_s+1 = (1+ lambda) * Sim_s, Sim_1 = 1 / d, 1 <= s <= S_end
    pow((1 + lambda), scale - 1) / dimensions
  }

  private def calLSHMatchPairs(minHashRDD: RDD[(String, Array[Int])], numBuckets: Int, jaccardSimilarity: Double) = {
    // The numHashTables is The length of the minHash signature
    val numHashTables = minHashRDD.first()._2.length
    // The numHashTables must be divisible by numBuckets
    val numRows: Int = numHashTables / numBuckets
    val bcNumOfRows = minHashRDD.context.broadcast(numRows)
    // Generate our pairs according to LSH for MinHash
    val bucketsRDD: RDD[((Int, Int), mutable.Iterable[(String, Array[Int])])] = minHashRDD.flatMap {
      case(id, signature) => signature.grouped(bcNumOfRows.value).zipWithIndex
        .map { case(band, bandIndex) => ((bandIndex, band.toList.hashCode), (id, signature))}
    }.aggregateByKey(mutable.Iterable.empty[(String, Array[Int])])((s, v) => s ++ Iterable(v), (i1, i2) => i1 ++ i2)

    val candidatePairsRDD: RDD[Set[(String, Array[Int])]] = bucketsRDD.flatMap {
      case((bandIndex, bucketId), cluster) => cluster.take(5).flatMap(doc1 => cluster.map(doc2 => Set(doc1, doc2)))
    }.cache()
//      .distinct().cache()

    // Look through all the pairs for matches
    val matchingPairsRDD: RDD[(String, String)] = candidatePairsRDD.map { pair =>
      if (pair.size == 1) {
        (pair, 1.0D)
      } else {
        (pair, minHashSimilarity(pair.head._2, pair.tail.head._2))
      }
    }.filter { case(pair, score) =>
      score >= jaccardSimilarity
    }.map { case(pair, score) =>
      if (pair.size == 1) {
        (pair.head._1, pair.head._1)
      } else {
        (pair.head._1, pair.tail.head._1)
      }
    }
    matchingPairsRDD
  }

  /**
    * Neighbour Computing
    * @param distinctCodes Non-repeating code in this "scale"
    * @param dimensions Data dimensions
    * @param codeModel Coding model
    * @param scale Weber scale
    * @param lambda Weber number
    * @param numBuckets The buckets number in LSH
    * @return Similar code pairs(SD coding model) or item ID pairs(MIN coding model)
    */
  def neighbourComputing(distinctCodes: RDD[(Int, String)],
                         dimensions: Int, codeModel: String, scale: Int, lambda: Double, numBuckets: Int
                        ): RDD[(String, String)] = {
    if (codeModel == "SD") {
      val codeAndIds = distinctCodes.map(_._2)
      val codeRelationPairs = calNeiCodePairsWithScale(codeAndIds, scale, dimensions)
      codeRelationPairs
    }
    else {
      val idAndCode = distinctCodes
      val jaccardLimit = calJaccardSimilarityLowerLimit(lambda, scale, dimensions)
      val minHashCodes = idAndCode.mapPartitions(iterator => iterator.map {
        case (id, codeString) => (id.toString, codeString.split(",").map(_.toInt))
      })
      val idRelationPairs = calLSHMatchPairs(minHashCodes, numBuckets, jaccardLimit)
      idRelationPairs
    }
  }

}
