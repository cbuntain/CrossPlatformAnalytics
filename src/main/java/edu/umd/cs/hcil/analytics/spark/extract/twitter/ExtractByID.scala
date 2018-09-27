package edu.umd.cs.hcil.analytics.spark.extract.twitter

import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/**
  * Created by cbuntain on 3/1/18.
  */
object ExtractByID {

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Tweet Extractor")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val idsPath = args(2)

    val tweetIds = scala.io.Source.fromFile(idsPath).getLines.map(idStr => idStr.toLong).toSet

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    val messages = if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }
    println("New Partition Count: " + messages.partitions.size)

    // Convert each JSON line in the file to a submission
    val tweetPairs : RDD[(String, Status)] = messages.map(line => {

      val status = TweetParser.parseJson(line)

      if ( status != null && status.getId != null ) {
        (line, status)
      } else {
        null
      }

    }).filter(tup => tup != null)

    val extractedTweets : RDD[String] = extractById(tweetIds, tweetPairs, sc).map(tup => tup._1)

    extractedTweets.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def extractById(idSet : Set[Long], tweets : RDD[(String,Status)], sc : SparkContext) : RDD[(String,Status)] = {
    val id_broadcast = sc.broadcast(idSet)

    return tweets.filter(tup => id_broadcast.value.contains(tup._2.getId))
  }

}
