package edu.umd.cs.hcil.analytics.spark.extract.twitter

import edu.umd.cs.hcil.analytics.utils.UrlDomainExtractor
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 4/27/18.
  */
object FilterUsers {
  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Filter Users by ID")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val userIdsPath = args(2)

    val userIds = scala.io.Source.fromFile(userIdsPath).getLines.map(idStr => idStr.toLong).toSet
    val userIds_broad = sc.broadcast(userIds)

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    val messages : RDD[String] = if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      println("New Partition Count: " + initialPartitions)

      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }

    val messagesFiltered = messages.filter(tweetStr => {
      val status = TweetParser.parseJson(tweetStr)
      if ( status != null && status.getUser != null ) {
        val userId = status.getUser.getId
        val localUserIds = userIds_broad.value

        localUserIds.contains(userId)
      } else {
        false
      }
    })

    messagesFiltered.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }
}
