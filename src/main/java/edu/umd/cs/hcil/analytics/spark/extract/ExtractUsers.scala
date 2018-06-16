package edu.umd.cs.hcil.analytics.spark.extract

import edu.umd.cs.hcil.models.fourchan.ThreadParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser.SubmissionModel
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 3/1/18.
  */
object ExtractUsers {

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
    val usernames : RDD[String] = messages.map(line => {


      if ( line.contains("is_self") ) {  // Test for Reddit data
      val submission = SubmissionParser.parseJson(line)
        if ( submission != null ) {
          submission.author.toLowerCase
        } else {
          null
        }
      } else if ( line.contains("\"resto\"") ) {  // Test for 4chan data
      val thread = ThreadParser.parseJson(line)
        if ( thread.posts != null && thread.posts.length > 0 ) {
          thread.posts.head.name.toLowerCase
        } else {
          null
        }
      } else {  // Assume Twitter data
      val status = TweetParser.parseJson(line)

        if ( status != null ) {
          status.getUser.getScreenName.toLowerCase
        } else {
          null
        }
      }

    }).filter(screenName => screenName != null && screenName.length > 0)

    val uniqueUsernames = usernames.distinct()

    uniqueUsernames.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }


}
