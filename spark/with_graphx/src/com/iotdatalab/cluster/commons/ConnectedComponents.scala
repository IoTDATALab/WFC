package com.iotdatalab.cluster.commons

import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

class ConnectedComponents extends Serializable {

  // Clustering using connectedComponents in Spark GraphX.
  def connectedComponents(codeRelationPairs:RDD[(String, String)], dimensions: Int, tag: String, scale: Int) = {
    val sc = codeRelationPairs.context
    if (tag == "LOW") {
      val relationshipRDD: RDD[(VertexId, VertexId)] = codeRelationPairs.mapPartitions(iterator =>
        iterator.map(x => (BigInt(x._1, 2).toLong,BigInt(x._2, 2).toLong)))
      val graph = Graph.fromEdgeTuples(relationshipRDD, defaultValue = 1)
      val cc = graph.connectedComponents()
      val intCodeAndCluster: VertexRDD[VertexId] = cc.vertices
      val length = dimensions * scale
      val formatStr = s"%0${length}d"
      val bcFormatStr = sc.broadcast(formatStr)
      val codeAndCluster = intCodeAndCluster.mapPartitions(iterator => iterator.map {
        case (intCode, cluster) => (bcFormatStr.value.format((BigInt(intCode.toBinaryString))), cluster.toInt.toString)
      })
      codeAndCluster
    }
    else {
      // For high dimensional version, the input 'codePairs' is idPairs actually.
      val relationshipRDD: RDD[(VertexId, VertexId)] = codeRelationPairs
        .map { case (src, des) => (src.toLong, des.toLong) }
      val graph = Graph.fromEdgeTuples(relationshipRDD, defaultValue = 1)
      val cc = graph.connectedComponents()
      val results = cc.vertices
      val idAndCluster = results.mapPartitions(iterator => iterator.map {
        case (id, cluster) => (id.toString, cluster.toInt.toString)
      })
      idAndCluster
    }
  }

}
