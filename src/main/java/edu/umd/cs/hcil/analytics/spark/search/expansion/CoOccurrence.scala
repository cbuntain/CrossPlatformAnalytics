package edu.umd.cs.hcil.analytics.spark.search.expansion

import edu.umd.cs.hcil.analytics.spark.search.twitter.TwitterKeywordFinder
import edu.umd.cs.hcil.analytics.tokenizer.twitter.TwitterTokenizer
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/**
  * Created by cbuntain on 10/2/18.
  */
object CoOccurrence {


  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KL Divergence")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val keywordPath = args(1)
    val topK = args(2).toInt

    val keywords = scala.io.Source.fromFile(keywordPath).getLines.toArray.map(s => s.split(" "))

    println("Queries:")
    keywords.foreach(s => println("\t" + s.reduce(_ + " " + _)))
    println()

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    // Repartition as necessary
    val messages = if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }
    println("New Partition Count: " + messages.partitions.size)

    // Convert each JSON line in the file to a status
    val items : RDD[(String, Status)] = messages.map(line => {
      (line, TweetParser.parseJson(line))
    }).filter(statusTuple => statusTuple != null && statusTuple._2 != null && statusTuple._2.getText != null)

    val foregroundData : RDD[Status] = TwitterKeywordFinder.keywordFilter(keywords, items).map(pair => pair._2)

    // Get frequencies of tokens and persist them
    val foreTokens = foregroundData
      .flatMap(status => TwitterTokenizer.tokenize(status, makeBigrams = true, preserveCase = false).map(token => (token, 1)))
      .reduceByKey(_+_)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val divergence = foreTokens.sortBy(tup => tup._2, false).take(topK)

    println("Top " + topK + " keywords:")
    for ( tokenTup <- divergence ) {
      println(tokenTup._1 + ", " + tokenTup._2)
    }
  }


}
