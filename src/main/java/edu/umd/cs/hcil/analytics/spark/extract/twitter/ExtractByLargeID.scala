package edu.umd.cs.hcil.analytics.spark.extract.twitter

import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/**
  * Created by cbuntain on 3/1/18.
  */
object ExtractByLargeID {

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Tweet Extractor")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val idsPath = args(2)

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
    val tweetPairs : RDD[(Long, String)] = messages.map(line => {

      val status = TweetParser.parseJson(line)

      if ( status != null && status.getId != null ) {
        (status.getId, line)
      } else {
        null
      }

    }).filter(tup => tup != null && tup._1 != null)

    val targetIds : RDD[Long] = sc.textFile(idsPath).map(s => s.toLong)

    val extractedTweets : RDD[String] = extractById(targetIds, tweetPairs).map(tup => tup._2)

    extractedTweets.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def extractById(idSet : RDD[Long], tweets : RDD[(Long,String)]) : RDD[(Long,String)] = {

    val idPairs = idSet.map(l => (l, 0)).reduceByKey((l, r) => 0).repartition(tweets.getNumPartitions)
    val tweetSingles = tweets.reduceByKey((l,r) => l)
    val joinedPairs = tweetSingles.join(idPairs).repartition(tweets.getNumPartitions*10)
    val filtered : RDD[(Long, String)] = joinedPairs.mapValues(tup => tup._1)

    return filtered
  }

}
