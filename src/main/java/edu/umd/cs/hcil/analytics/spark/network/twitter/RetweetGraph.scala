package edu.umd.cs.hcil.analytics.spark.network.twitter

import edu.umd.cs.hcil.analytics.spark.network.{DefaultGraph, UserNode}
import org.apache.spark.graphx.{Graph, VertexId, Edge}
import org.apache.spark.rdd.RDD
import twitter4j.Status

/**
  * Created by cbuntain on 9/26/18.
  */
object RetweetGraph extends DefaultGraph {

  def getGraph(tweets : RDD[Status]) : Graph[UserNode, Long] = {
    return getGraph(tweets, 0)
  }

  def getGraph(tweets : RDD[Status], minDegree: Int) : Graph[UserNode, Long] = {

    // Map retweets to edges, using a reduceByKey operation to merge
    //  retweets between two users, so we can get weights based on
    //  retweet counts
    val retweet_edges : RDD[(UserNode, UserNode, Long)] = tweets.filter(status => status.isRetweet).map(status => {
      val source = UserNode(status.getUser.getId, status.getUser.getScreenName)
      val sink = UserNode(status.getRetweetedStatus.getUser.getId, status.getRetweetedStatus.getUser.getScreenName)

      val pair_id = "%d,%d".format(source.id, sink.id).hashCode

      (pair_id, (source, sink, 1L))
    }).reduceByKey((l, r) => {
      val src = l._1
      val dst = l._2

      (src, dst, l._3 + r._3)
    }).map(tup => tup._2)

    // Persist the retweet_edges RDD because we need to traverse it twice
    retweet_edges.cache()

    val graph = edgesToGraph(retweet_edges, minDegree)

    return graph
  }
}
