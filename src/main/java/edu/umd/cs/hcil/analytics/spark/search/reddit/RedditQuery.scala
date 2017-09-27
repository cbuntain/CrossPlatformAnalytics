package edu.umd.cs.hcil.analytics.spark.search.reddit

import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser.SubmissionModel
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.index.memory.MemoryIndex
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by cbuntain on 9/14/17.
  */
object RedditQuery {


  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Lucene Tweet Query")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)
    val keywordPath = args(2)

    val queries = scala.io.Source.fromFile(keywordPath).getLines.toList

    println("Queries:")
    queries.foreach(s => println("\t" + s))
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

    val relevantJson = querier(queries, textFields, 0d).map(pair => pair._1)

    relevantJson.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

  def querier(queries : List[String], statusList : RDD[(String,SubmissionModel)], threshold : Double) : RDD[(String,SubmissionModel)] = {
    // Pseudo-Relevance feedback
    val scoredPairs = statusList.mapPartitions(iter => {
      // Construct an analyzer for our tweet text
      val analyzer = new StandardAnalyzer()
      val parser = new StandardQueryParser(analyzer)

      // words with spaces between them must appear using the following connective (AND or OR)
      parser.setDefaultOperator(org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator.AND)

      iter.map(pair => {
        val submission = pair._2
        val text = submission.title + ", " + (if ( submission.selftext != null ) { submission.selftext } else { "" })

        // Construct an in-memory index for the tweet data
        val idx = new MemoryIndex()

        idx.addField("content", text.toLowerCase(), analyzer)

        var score = 0.0d
        for ( q <- queries ) {
          score = score + idx.search(parser.parse(q, "content"))
        }

        (pair, score)
      })
    }).filter(tuple => tuple._2 > threshold)
      .map(tuple => tuple._1)

    return scoredPairs
  }

}
