package edu.umd.cs.hcil.analytics.spark.search.reddit

import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser.SubmissionModel
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 9/14/17.
  */
object RedditKeywordFinder {


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

    // Convert each JSON line in the file to a submission
    val textFields : RDD[(String, SubmissionModel)] = messages.map(line => {

      if ( line.contains("subreddit_id") ) {  // Test for Reddit data
        val submission = SubmissionParser.parseJson(line)
        (line, submission)
      } else {
        null
      }
    }).filter(sub => sub != null && sub._2 != null && sub._2.title.length > 0)

    val relevantJson = textFields.filter(pair => keywordFilter(keywords, pair._2)).map(pair => pair._1)

    relevantJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def keywordFilter(keywords : Array[Array[String]], submission : SubmissionModel) : Boolean = {
    val text = submission.title + ", " + (if ( submission.selftext != null ) { submission.selftext } else { "" }).toLowerCase

    val intersection = keywords.filter(k_list => k_list.filter(k => text.contains(k)).length == k_list.length )

    return intersection.length > 0
  }

}
