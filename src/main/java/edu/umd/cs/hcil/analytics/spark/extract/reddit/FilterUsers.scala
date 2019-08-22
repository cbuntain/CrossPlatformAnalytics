package edu.umd.cs.hcil.analytics.spark.extract.reddit

import edu.umd.cs.hcil.models.reddit.{CommentParser, RedditModel, SubmissionParser}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 9/14/17.
  */
object FilterUsers {


  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Extract Reddit Posts by User")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val userPath = args(2)

    val users = scala.io.Source.fromFile(userPath).getLines.toSet

    println("Users:")
    users.foreach(s => println("\t" + s))
    println()

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    val messages = if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      println("New Partition Count: " + initialPartitions)

      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }

    // Convert each JSON line in the file to a submission
    val textFields : RDD[(String, RedditModel)] = messages.map(line => {

      if ( line.contains("parent_id") ) {  // Test for Reddit data. Only comments have parents
        val submission : RedditModel = CommentParser.parseJson(line)
        (line, submission)
      } else {
        val comment : RedditModel = SubmissionParser.parseJson(line)
        (line, comment)
      }
    }).filter(sub => sub != null && sub._2 != null && sub._2.text.getOrElse("").length > 0)

    val relevantJson : RDD[String] = textFields.filter(tup => users.contains(tup._2.author.toLowerCase)).map(tup => tup._1)

    relevantJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }


}
