package edu.umd.cs.hcil.analytics.spark.extract.twitter

import edu.umd.cs.hcil.analytics.utils.UrlDomainExtractor
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/**
  * Created by cbuntain on 4/26/18.
  */
object FindUsersByLink {
  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hyperlink User To Domain")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val urlPath = args(2)

    val urlSet = scala.io.Source.fromFile(urlPath).getLines.toSet

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    val messages : RDD[String] = if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      println("New Partition Count: " + initialPartitions)

      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }

    val users = messages.flatMap(tweetStr => {
      val status = TweetParser.parseJson(tweetStr)

      if ( status != null && status.getUser != null ) {
        val userId = status.getUser.getId
        val urls = getUrls(status)

        urls.
          filter(url => url.length > 0 ).
          map(url => UrlDomainExtractor.getTLD(url)).
          filter(url => url.nonEmpty).
          map(tld => (userId, tld.get))
      } else {
        List[(Long,String)]()
      }
    }).filter(tup => urlSet.contains(tup._2)).map(tup => "%d".format(tup._1))

    users.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def getUrls(tweet : Status) : Array[String] = {
    tweet.getURLEntities.filter(urlEntity => urlEntity.getURL.length > 0).map(urlEntity => urlEntity.getUnshortenedURL)
  }
}
