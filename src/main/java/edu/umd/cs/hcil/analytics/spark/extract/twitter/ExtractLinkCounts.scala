package edu.umd.cs.hcil.analytics.spark.extract.twitter

import edu.umd.cs.hcil.analytics.utils.UrlDomainExtractor
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import org.apache.commons.csv.CSVFormat

/**
  * Created by cbuntain on 4/26/18.
  */
object ExtractLinkCounts {
  /**
    * Given a set of tweets in JSON format, extract the links shared
    * and save their counts
    *
    * @param args the command line arguments
    *
    *
    *
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hyperlink User To Domain")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

    val messages = sc.textFile(dataPath)

    val topK : Option[Int] = if ( args.size > 2 ) {
      Some(args(2).toInt)
    } else {
      None
    }

    val linkCounts = messages.flatMap(tweetStr => {
      val status = TweetParser.parseJson(tweetStr)

      if ( status != null && status.getUser != null ) {
        getUrls(status).map(url => (url, 1))
      } else {
        Array[(String, Int)]()
      }
    }).reduceByKey(_+_)

    val relLinkCounts = if ( topK.isDefined ) {
      sc.parallelize(linkCounts.takeOrdered(topK.get)(Ordering[Int].reverse.on(tup => tup._2)))
    } else {
      linkCounts
    }

    relLinkCounts
      .map(tup => "%s,%d".format(CSVFormat.EXCEL.format(tup._1), tup._2))
      .saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def getUrls(tweet : Status) : Array[String] = {
    var mainLinks = tweet.getURLEntities.filter(urlEntity => urlEntity.getURL.length > 0).map(urlEntity => urlEntity.getExpandedURL)

    if (tweet.isRetweet) {
      mainLinks = mainLinks ++ getUrls(tweet.getRetweetedStatus)
    }

    if (tweet.getQuotedStatus != null) {
      mainLinks = mainLinks ++ getUrls((tweet.getQuotedStatus))
    }

    mainLinks.map(cleanUrl).distinct
  }

  def cleanUrl(url : String) : String = {
    if ( url.endsWith("/") ) {
      url.substring(0, url.length - 1)
    } else {
      url
    }
  }
}
