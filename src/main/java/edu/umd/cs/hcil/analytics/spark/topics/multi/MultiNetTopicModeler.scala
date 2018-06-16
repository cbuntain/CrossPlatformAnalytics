package edu.umd.cs.hcil.analytics.spark.topics.multi

import edu.umd.cs.hcil.analytics.spark.topics.TopicModelLDA
import edu.umd.cs.hcil.models.fourchan.ThreadParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 8/23/17.
  */
object MultiNetTopicModeler {

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Submission Topic Model")
    val sc = new SparkContext(conf)

    // Parse config
    val confOpt = TopicModelLDA.parser.parse(args, TopicModelLDA.Config())
    if ( !confOpt.isDefined ) {
      sys.exit(-1)
    }
    val argConf : TopicModelLDA.Config = confOpt.get
    println("Configuration:")
    println(argConf)

    val dataPath = argConf.inputPath
    val outputPath = argConf.outputPath
    val stopwordPath = argConf.stopwordFile

    val stopwords = scala.io.Source.fromFile(stopwordPath).getLines.toList


    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    // Repartition if desired using the new partition count
    val messages = if ( argConf.numPartitions > 0 ) {
      val initialPartitions = argConf.numPartitions
      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }
    val newPartitionSize = messages.partitions.size
    println("New Partition Count: " + newPartitionSize)

    // Convert each JSON line in the file to a submission
    val textFields : RDD[String] = messages.map(line => {

      if ( line.contains("subreddit_id") ) {  // Test for Reddit data
        val submission = SubmissionParser.parseJson(line)
        if ( submission != null ) {
          submission.title
        } else {
          null
        }
      } else if ( line.contains("\"resto\"") ) {  // Test for 4chan data
        val thread = ThreadParser.parseJson(line)
        if ( thread.posts != null && thread.posts.length > 0 ) {
          ThreadParser.getPlainText(thread.posts.head)
        } else {
          null
        }
      } else {  // Assume Twitter data
        val status = TweetParser.parseJson(line)

        if ( status != null &&
          status.isRetweet == false &&
          status.getLang != null &&
          status.getLang.compareToIgnoreCase("en") == 0 &&
          TweetParser.getHashtagCount(status) <= 3 &&
          TweetParser.getUrlCount(status) <= 2 ) {
          status.getText
        } else {
          null
        }
      }
    }).filter(sub => sub != null)

    val textWithIds = textFields.zipWithUniqueId().map(tup => (tup._2, tup._1))

    val ldaTuple = TopicModelLDA.runLda(
      argConf.numTopics,
      textWithIds,
      stopwords.toArray,
      sc,
      argConf.maxIterations,
      argConf.minTF,
      argConf.minDF)
    val ldaModel = ldaTuple._1
    val cvModel = ldaTuple._2

    ldaModel.save(sc, outputPath + "_lda.model")
    cvModel.save(outputPath + "_vectorizer.model")

    TopicModelLDA.printResults(ldaModel, cvModel.vocabulary)
  }

}
