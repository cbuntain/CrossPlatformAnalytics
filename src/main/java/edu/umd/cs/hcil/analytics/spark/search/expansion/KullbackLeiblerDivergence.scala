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
object KullbackLeiblerDivergence {


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

    val totalCount = items.count()

    val foregroundData : RDD[Status] = TwitterKeywordFinder.keywordFilter(keywords, items).map(pair => pair._2)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val relevantCount = foregroundData.count()

    val sampleRatio : Double = relevantCount.toDouble / totalCount.toDouble
    println("Sample Ratio: %.4f".format(sampleRatio))

    // Sample the background data
    val backgroundSample : RDD[Status] = items.sample(false, sampleRatio).map(tup => tup._2)

    // Get frequencies of tokens and persist them
    val foreTokens = foregroundData
      .flatMap(status => TwitterTokenizer.tokenize(status, makeBigrams = true, preserveCase = false).map(token => (token, 1)))
      .reduceByKey(_+_)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val backTokens = backgroundSample
      .flatMap(status => TwitterTokenizer.tokenize(status, makeBigrams = true, preserveCase = false).map(token => (token, 1)))
      .reduceByKey(_+_)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // unpersist things we don't need
    foregroundData.unpersist(true)

    val divergence = score(foreTokens, backTokens).sortBy(tup => tup._2, false).take(topK)

    println("Top " + topK + " divergent keywords:")
    for ( tokenTup <- divergence ) {
      println(tokenTup._1 + ", " + tokenTup._2)
    }
  }

  /**
    * Given an RDD of tokens and their frequencies, compare it against
    * the frequencies in a background model and score keywords by
    * how divergent they are
    *
    * @param foreground RDD of foreground tokens and counts
    */
  def score(foreground : RDD[(String, Int)], background : RDD[(String, Int)]) : RDD[(String, Double)] = {

    // what's the total number of all tokens in both foreground and background models?
    val foreTotal : Double = foreground.map(t => t._2).reduce((l, r) => l + r).toDouble
    val backTotal : Double = background.map(t => t._2).reduce((l, r) => l + r).toDouble

    // Convert raw counts to proportions
    val foreDist = foreground.map(tup => (tup._1, tup._2.toDouble / foreTotal))
    val backDist = background.map(tup => (tup._1, tup._2.toDouble / backTotal))

    // for each token in foreground, match it with its background frequency
    //  and calculate the log ratio times the foreground proportion
    //  NOTE: We need to handle cases where a token exists in the foreground
    //  but not the background since this function can work with background samples
    //  that may be incomplete
    val tokenDivergence = foreDist.leftOuterJoin(backDist).map(tup => {
      val token = tup._1
      val freqs = tup._2

      val probFore : Double = freqs._1
      val probBack : Double = freqs._2.getOrElse(1.0/backTotal) // we default to assuming freq 1 if we don't have this keyword

      val klDiv : Double = probFore * (math.log(probFore) - math.log(probBack))

      (token, klDiv)
    })

    return tokenDivergence
  }

}
