package edu.umd.cs.hcil.analytics.spark.search.reddit

import edu.umd.cs.hcil.models.reddit.{CommentParser, RedditModel, SubmissionParser}
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
    val textFields : RDD[(String, RedditModel)] = messages.map(line => {

      if ( line.contains("parent_id") ) {  // Test for Reddit data. Only comments have parents
        val submission : RedditModel = CommentParser.parseJson(line)
        (line, submission)
      } else {
        val comment : RedditModel = SubmissionParser.parseJson(line)
        (line, comment)
      }
    }).filter(sub => sub != null && sub._2 != null && sub._2.text.getOrElse("").length > 0)

    val relevantJson = keywordFilter(keywords, textFields).map(pair => pair._1)

    relevantJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def keywordFilter(keywords : Array[Array[String]], items : RDD[(String, RedditModel)]) : RDD[(String, RedditModel)] = {

    items.filter(statusTup => {
      val item = statusTup._2
      val text = item.text.getOrElse("").toLowerCase

      val intersection = keywords.filter(k_list => k_list.filter(k => text.contains(k)).length == k_list.length)

      intersection.length > 0
    })
  }

}
