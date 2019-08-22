package edu.umd.cs.hcil.analytics.spark.network

import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by cbuntain on 9/26/18.
  */
trait DefaultGraph {

  def edgesToGraph(edgeRdd : RDD[(UserNode, UserNode, Long)], minDegree : Int, cacheStrategy : StorageLevel = StorageLevel.MEMORY_ONLY ) : Graph[UserNode, Long] = {

    val users : RDD[(VertexId, UserNode)] = edgeRdd.flatMap(edge => Array(
      (edge._1.id, edge._1),
      (edge._2.id, edge._2)
    ))

    val edges : RDD[Edge[Long]] = edgeRdd.map(edge => Edge(edge._1.id, edge._2.id, edge._3))

    // Instantiate the graph and trim nodes with fewer degree than our limit
    val graph : Graph[UserNode, Long] = if ( minDegree == 0 ) {
      Graph(users, edges, defaultVertexAttr = null, vertexStorageLevel = cacheStrategy, edgeStorageLevel = cacheStrategy)

    } else {
      // local copy of the graph
      val local_graph = GraphImpl.apply(users, edges, defaultVertexAttr = null, vertexStorageLevel = cacheStrategy, edgeStorageLevel = cacheStrategy)

      // get degrees
      val degree_graph = local_graph.degrees
      val valid_vertices = degree_graph.filter(degree_tup => degree_tup._2 >= minDegree).map(tup => tup._1).collect().toSet
      local_graph.unpersist(false)

      // Constrain to only users and edges with adequate degree
      //  Ideally, we would use subgraph() here, but this led to problems for me
      //  where Spark would error out with an array out of bounds exception
      val valid_users = users.filter(node_tup => valid_vertices.contains(node_tup._1))
      val valid_edges = edges.filter(e => valid_vertices.contains(e.srcId) && valid_vertices.contains(e.dstId))

      // Create a new subgraph from these valid users
      val valid_graph = GraphImpl.apply(valid_users, valid_edges, defaultVertexAttr = null, vertexStorageLevel = cacheStrategy, edgeStorageLevel = cacheStrategy)

      valid_graph
    }

    return graph
  }

}
