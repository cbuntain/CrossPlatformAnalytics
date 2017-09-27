package edu.umd.cs.hcil.analytics.spark.topics

import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.mllib.clustering.LocalLDAModel

/**
  * Created by cbuntain on 7/27/17.
  */
object EvaluateModel {

  case class Config(inputDataPath : String = "",
                    inputModelPath : String = "",
                    inputVectorPath : String = "",
                    stopwordFile : String = "",
                    sampleSize : Int = 100,
                    numPartitions : Int = -1)

  val parser = new scopt.OptionParser[Config]("ModelEvaluator"){

    head("ModelEvaluator", "1.0a")

    opt[String]('d', "data").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputDataPath = x)} )
      .text("Input data file")

    opt[String]('m', "model").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputModelPath = x)} )
      .text("Input model file")

    opt[String]('v', "vectorizer").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputVectorPath = x)} )
      .text("Input vectorizer file")

    opt[String]('s', "stopFile").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(stopwordFile = x)} )
      .text("File of Stopwords")

    opt[Int]('n', "sampleSize")
      .valueName("<integer>")
      .action( (x, c) => {c.copy(sampleSize = x)} )
      .text("Number of samples for calculating likelihood")

    opt[Int]('p', "partitions")
      .valueName("<value>")
      .action( (x, c) => c.copy(numPartitions = x) )
      .text("Number of partitions in to which divide the data (defaults to data size)")
  }

  def main(args : Array[String]) : Unit = {


    val conf = new SparkConf().setAppName("Topic Model Evaluation")
    val sc = new SparkContext(conf)

    // Parse config
    val confOpt = parser.parse(args, Config())
    if ( !confOpt.isDefined ) {
      sys.exit(-1)
    }
    val argConf : Config = confOpt.get

    val dataPath = argConf.inputDataPath
    val modelPath = argConf.inputModelPath
    val vectorizerPath = argConf.inputVectorPath
    val stopwordPath = argConf.stopwordFile

    val stopwords = scala.io.Source.fromFile(stopwordPath).getLines.toArray

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
      if ( line.contains("subreddit_id") ) {
        val submission = SubmissionParser.parseJson(line)
        if ( submission != null ) {
          submission.title
        } else {
          null
        }
      } else {
        val tweet = TweetParser.parseJson(line)

        if ( tweet != null ) {
          tweet.getText
        } else {
          null
        }
      }
    }).filter(sub => sub != null)

    val localModel : LocalLDAModel = try {
      println("Attemping to load distributed model...")
      val model = DistributedLDAModel.load(sc, modelPath)
      println("Distributed model loaded.")
      model.toLocal
    } catch {
      case e : Exception => {
        println("Attemping to load LOCAL model...")
        val model = LocalLDAModel.load(sc, modelPath)
        println("Local model loaded.")

        model
      }
    }
    val cvModel = CountVectorizerModel.load(vectorizerPath)

    val textWithIds = if ( argConf.sampleSize > 0 ) {
      val proportion = argConf.sampleSize.toDouble / textFields.count.toDouble
      textFields.sample(withReplacement = true, fraction = proportion).zipWithUniqueId().map(tup => tup.swap)
    } else {
      textFields.zipWithUniqueId().map(tup => tup.swap)
    }

    val filteredTokens = TopicModelLDA.textToVector(textWithIds, stopwords, sc)
    val testDocuments = cvModel.transform(filteredTokens)
      .select("docId", "features")
      .rdd
      .map { case Row(docId: Long, features: MLVector) => (docId, Vectors.fromML(features)) }
      .cache()


//    val perplexity = localModel.logPerplexity(testDocuments)
//    println("Achieved Perplexity:")
//    println("\tDocument Count: " + testDocuments.count())
//    println("\tPerplexity: " + perplexity)

    val logLikelihood = localModel.logLikelihood(testDocuments)
    println("Achieved Log-Likelihood:")
    println("\tDocument Count: " + testDocuments.count())
    println("\tLog-Likelihood: " + logLikelihood)
  }

}
