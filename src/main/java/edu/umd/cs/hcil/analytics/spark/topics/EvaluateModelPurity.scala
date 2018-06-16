package edu.umd.cs.hcil.analytics.spark.topics

import edu.umd.cs.hcil.models.fourchan.ThreadParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 7/27/17.
  */
object EvaluateModelPurity {

  case class Config(inputTwitterDataPath : String = "",
                    inputRedditDataPath : String = "",
                    input4chanDataPath : String = "",
                    inputModelPath : String = "",
                    inputVectorPath : String = "",
                    stopwordFile : String = "",
                    sampleSize : Int = 100,
                    numPartitions : Int = -1)

  val parser = new scopt.OptionParser[Config]("ModelEvaluator"){

    head("ModelPurityEvaluator", "1.0a")

    opt[String]('t', "twitter").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputTwitterDataPath = x)} )
      .text("Input data file for Twitter")

    opt[String]('r', "reddit").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputRedditDataPath = x)} )
      .text("Input data file for Reddit")

    opt[String]('f', "4chan").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(input4chanDataPath = x)} )
      .text("Input data file for 4chan")

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

    val twitterDataPath = argConf.inputTwitterDataPath
    val redditDataPath = argConf.inputRedditDataPath
    val fourchanDataPath = argConf.input4chanDataPath
    val modelPath = argConf.inputModelPath
    val vectorizerPath = argConf.inputVectorPath
    val stopwordPath = argConf.stopwordFile

    val stopwords = scala.io.Source.fromFile(stopwordPath).getLines.toArray

    // Load the model
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

    // Read in TWitter data
    val twitterMessagesRaw = sc.textFile(twitterDataPath)
    println("Initial Twitter Partition Count: " + twitterMessagesRaw.partitions.size)

    // Repartition if desired using the new partition count
    val twitterMessages = if ( argConf.numPartitions > 0 ) {
      val initialPartitions = argConf.numPartitions
      twitterMessagesRaw.repartition(initialPartitions)
    } else {
      twitterMessagesRaw
    }

    // Convert each JSON line in the file to a submission
    val twitterTextFields : RDD[String] = twitterMessages.map(line => {
      val tweet = TweetParser.parseJson(line)

      if ( tweet != null ) {
        tweet.getText
      } else {
        null
      }
    }).filter(sub => sub != null)

    val twitterSampleData = if ( argConf.sampleSize > 0 ) {
      val proportion = argConf.sampleSize.toDouble / twitterTextFields.count.toDouble
      twitterTextFields.sample(withReplacement = true, fraction = proportion).zipWithUniqueId().map(tup => tup.swap)
    } else {
      twitterTextFields.zipWithUniqueId().map(tup => tup.swap)
    }

    val twitterFilteredTokens = TopicModelLDA.textToVector(twitterSampleData, stopwords, sc)
    val twitterDocuments = cvModel.transform(twitterFilteredTokens)
      .select("docId", "features")
      .rdd
      .map { case Row(docId: Long, features: MLVector) => (docId, Vectors.fromML(features)) }
      .cache()
    val twitterMaxTopicAssignments = localModel.topicDistributions(twitterDocuments).map(tup => tup._2.argmax)
    val twitterTopicCounts = twitterMaxTopicAssignments.map(topic => (topic, 1)).reduceByKey((l, r) => l+r).collect.toMap

    // Read in Reddit data
    val redditMessagesRaw = sc.textFile(redditDataPath)
    println("Initial Reddit Partition Count: " + redditMessagesRaw.partitions.size)

    // Repartition if desired using the new partition count
    val redditMessages = if ( argConf.numPartitions > 0 ) {
      val initialPartitions = argConf.numPartitions
      redditMessagesRaw.repartition(initialPartitions)
    } else {
      redditMessagesRaw
    }

    // Convert each JSON line in the file to a submission
    val redditTextFields : RDD[String] = redditMessages.map(line => {
      val submission = SubmissionParser.parseJson(line)
      if ( submission != null ) {
        submission.title
      } else {
        null
      }
    }).filter(sub => sub != null)

    val redditSampleData = if ( argConf.sampleSize > 0 ) {
      val proportion = argConf.sampleSize.toDouble / redditTextFields.count.toDouble
      redditTextFields.sample(withReplacement = true, fraction = proportion).zipWithUniqueId().map(tup => tup.swap)
    } else {
      redditTextFields.zipWithUniqueId().map(tup => tup.swap)
    }

    val redditFilteredTokens = TopicModelLDA.textToVector(redditSampleData, stopwords, sc)
    val redditDocuments = cvModel.transform(redditFilteredTokens)
      .select("docId", "features")
      .rdd
      .map { case Row(docId: Long, features: MLVector) => (docId, Vectors.fromML(features)) }
      .cache()

    val redditMaxTopicAssignments = localModel.topicDistributions(redditDocuments).map(tup => tup._2.argmax)
    val redditTopicCounts = redditMaxTopicAssignments.map(topic => (topic, 1)).reduceByKey((l, r) => l+r).collect.toMap


    // Read in 4chan data
    val fourchanMessagesRaw = sc.textFile(fourchanDataPath)
    println("Initial 4chan Partition Count: " + fourchanMessagesRaw.partitions.size)

    // Repartition if desired using the new partition count
    val fourchanMessages = if ( argConf.numPartitions > 0 ) {
      val initialPartitions = argConf.numPartitions
      fourchanMessagesRaw.repartition(initialPartitions)
    } else {
      fourchanMessagesRaw
    }

    // Convert each JSON line in the file to a submission
    val fourchanTextFields : RDD[String] = fourchanMessages.map(line => {
      val thread = ThreadParser.parseJson(line)
      if ( thread != null && thread.posts != null && thread.posts.length > 0 && thread.posts.head != null) {
        ThreadParser.getPlainText(thread.posts.head)
      } else {
        null
      }
    }).filter(sub => sub != null)

    val fourchanSampleData = if ( argConf.sampleSize > 0 ) {
      val proportion = argConf.sampleSize.toDouble / fourchanTextFields.count.toDouble
      fourchanTextFields.sample(withReplacement = true, fraction = proportion).zipWithUniqueId().map(tup => tup.swap)
    } else {
      fourchanTextFields.zipWithUniqueId().map(tup => tup.swap)
    }

    val fourchanFilteredTokens = TopicModelLDA.textToVector(fourchanSampleData, stopwords, sc)
    val fourchanDocuments = cvModel.transform(fourchanFilteredTokens)
      .select("docId", "features")
      .rdd
      .map { case Row(docId: Long, features: MLVector) => (docId, Vectors.fromML(features)) }
      .cache()

    val fourchanMaxTopicAssignments = localModel.topicDistributions(fourchanDocuments).map(tup => tup._2.argmax)
    val fourchanTopicCounts = fourchanMaxTopicAssignments.map(topic => (topic, 1)).reduceByKey((l, r) => l+r).collect.toMap


    println("Topic,Twitter,Reddit,4chan")
    for ( topicIndex <- 0 until localModel.k ) {
      println("%d,%d,%d,%d".format(
        topicIndex,
        twitterTopicCounts.getOrElse(topicIndex,0),
        redditTopicCounts.getOrElse(topicIndex, 0),
        fourchanTopicCounts.getOrElse(topicIndex, 0)
      ))
    }
  }

}
