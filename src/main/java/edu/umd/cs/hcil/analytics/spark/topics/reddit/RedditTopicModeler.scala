package edu.umd.cs.hcil.analytics.spark.topics.reddit

import java.util.Date

import edu.umd.cs.hcil.analytics.spark.topics.TopicModelLDA
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import edu.umd.cs.hcil.analytics.utils.{DateFormatter, TimeScale}
import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser.SubmissionModel

/**
  * Created by cbuntain on 7/20/17.
  */
object RedditTopicModeler {

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
    val submissions : RDD[SubmissionModel] = messages.map(line => {
      SubmissionParser.parseJson(line)
    }).filter(sub => sub != null)

    val submissionIds = submissions.zipWithUniqueId().map(tup => (tup._2, tup._1.title))

    val ldaTuple = TopicModelLDA.runLda(
      argConf.numTopics,
      submissionIds,
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
