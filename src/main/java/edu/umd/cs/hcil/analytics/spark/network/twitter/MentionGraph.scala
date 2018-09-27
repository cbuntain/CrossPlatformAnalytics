package edu.umd.cs.hcil.analytics.spark.network.twitter

import edu.umd.cs.hcil.analytics.spark.network.{DefaultGraph, UserNode}
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import twitter4j.Status

/**
  * Created by cbuntain on 9/26/18.
  */
object MentionGraph extends DefaultGraph {

  def getGraph(tweets : RDD[Status]) : Graph[UserNode, Long] = {
    return getGraph(tweets, 0)
  }

  def getGraph(tweets : RDD[Status], minDegree: Int) : Graph[UserNode, Long] = {

    // Map retweets to edges, using a reduceByKey operation to merge
    //  retweets between two users, so we can get weights based on
    //  retweet counts
    val mention_edges : RDD[(UserNode, UserNode, Long)] = tweets.flatMap(status => {
      val source = UserNode(status.getUser.getId, status.getUser.getScreenName)

      status.getUserMentionEntities.map(entity => {
        val sink = UserNode(entity.getId, entity.getScreenName)

        val pair_id = "%d,%d".format(source.id, sink.id).hashCode
        (pair_id, (source, sink, 1L))
      })
    }).reduceByKey((l, r) => {
      val src = l._1
      val dst = l._2

      (src, dst, l._3 + r._3)
    }).map(tup => tup._2)

    // Persist the retweet_edges RDD because we need to traverse it twice
    mention_edges.cache()

    val graph = edgesToGraph(mention_edges, minDegree)

    return graph
  }
}
