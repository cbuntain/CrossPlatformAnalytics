package edu.umd.cs.hcil.analytics.spark.extract.twitter

import edu.umd.cs.hcil.models.fourchan.ThreadParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 3/1/18.
  */
object ExtractUserIDs {

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Username Extractor")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    var messages = messagesRaw
    if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      messages = messagesRaw.repartition(initialPartitions)
      println("New Partition Count: " + messages.partitions.size)
    }

    // Convert each JSON line in the file to a submission
    val usernames : RDD[(Long,Set[String])] = messages.map(line => {

      val status = TweetParser.parseJson(line)

      if ( status != null && status.getUser != null) {
        (status.getUser.getId, Set(status.getUser.getScreenName.toLowerCase))
      } else {
        null
      }

    }).filter(tup => tup != null).reduceByKey(_ ++ _)

    val userStrings : RDD[String] = usernames.map(tup => {
      val uid = tup._1
      val usernames = tup._2.map(s => "\"%s\"".format(s)).reduce((l, r) => "%s,%s".format(l, r))

      "{\"userid\": %d, \"usernames\":[%s]}".format(uid, usernames)
    })

    userStrings.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }


}
