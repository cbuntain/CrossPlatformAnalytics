package edu.umd.cs.hcil.analytics.spark.network

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
  * Created by cbuntain on 9/26/18.
  */
trait DefaultGraph {

  def edgesToGraph(edgeRdd : RDD[(UserNode, UserNode, Long)], minDegree : Int) : Graph[UserNode, Long] = {

    val users : RDD[(VertexId, UserNode)] = edgeRdd.flatMap(edge => Array(
      (edge._1.id, edge._1),
      (edge._2.id, edge._2)
    ))

    val edges : RDD[Edge[Long]] = edgeRdd.map(edge => Edge(edge._1.id, edge._2.id, edge._3))

    // Instantiate the graph and trim nodes with fewer degree than our limit
    val graph : Graph[UserNode, Long] = if ( minDegree == 0 ) {
      Graph(users, edges)

    } else {
      // local copy of the graph
      val local_graph = Graph(users, edges)

      // Join graph with degree list, so each node now knows its degree
      //  Resulting graph has a new vertex type with an extra field for degree
      val degree_graph : Graph[(UserNode, Int), Long] = local_graph
        .outerJoinVertices(local_graph.degrees)((vId, node, degreeOpt) => (node, degreeOpt.getOrElse(0)))

      // Identify nodes whose degree equals or exceeds our threshold
      val sub_degree_graph = degree_graph.subgraph(vpred = (vId, tup) => tup._2 > minDegree)

      // Remove the degree attribute
      sub_degree_graph.mapVertices((id, attr) => attr._1)
    }

    return graph
  }

}
