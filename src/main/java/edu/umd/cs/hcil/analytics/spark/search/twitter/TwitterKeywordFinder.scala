package edu.umd.cs.hcil.analytics.spark.search.twitter

import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser.SubmissionModel
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/**
  * Created by cbuntain on 9/14/17.
  */
object TwitterKeywordFinder {


  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Lucene Tweet Query")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val keywordPath = args(2)

    val keywords = scala.io.Source.fromFile(keywordPath).getLines.toArray.map(s => s.split(" "))

    println("Queries:")
    keywords.foreach(s => println("\t" + s.reduce(_ + " " + _)))
    println()

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    var messages = messagesRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      messages = messagesRaw.repartition(initialPartitions)
      println("New Partition Count: " + messages.partitions.size)
    }

    // Convert each JSON line in the file to a status
    val items : RDD[(String, Status)] = messages.map(line => {

      (line, TweetParser.parseJson(line))
    }).filter(statusTuple => statusTuple != null && statusTuple._2 != null)

    val relevantJson = keywordFilter(keywords, items).map(pair => pair._1)

    relevantJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def keywordFilter(keywords : Array[Array[String]], items : RDD[(String, Status)]) : RDD[(String, Status)] = {

    items.filter(statusTup => {
      val status = statusTup._2
      val text = status.getText + ", " + (if (status.isRetweet) {
        status.getRetweetedStatus.getText
      } else {
        ""
      }).toLowerCase

      val intersection = keywords.filter(k_list => k_list.filter(k => text.contains(k)).length == k_list.length)

      intersection.length > 0
    })
  }

}
