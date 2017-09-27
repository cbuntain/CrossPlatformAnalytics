package edu.umd.cs.hcil.analytics.spark.topics

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{LDA, LDAModel, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by cbuntain on 7/20/17.
  */
object TopicModelLDA {

  case class Config(inputPath : String = "",
                    outputPath : String = "",
                    stopwordFile : String = "",
                    numPartitions : Int = -1,
                    numTopics: Int = 10,
                    maxIterations: Int = 10,
                    minTF: Int = 10,
                    minDF: Int = 10)

  val parser = new scopt.OptionParser[Config]("TopicModelLDA"){

    head("TopicModelLDA", "1.0a")

    opt[String]('i', "input").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputPath = x)} )
      .text("Input file")

    opt[String]('o', "output").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(outputPath = x)} )
      .text("Output file")

    opt[String]('s', "stopFile").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(stopwordFile = x)} )
      .text("File of Stopwords")

    opt[Int]('p', "partitions")
      .valueName("<value>")
      .action( (x, c) => c.copy(numPartitions = x) )
      .text("Number of partitions in to which divide the data (defaults to data size)")

    opt[Int]('t', "topics").required()
      .valueName("<value>")
      .action( (x, c) => c.copy(numTopics = x) )
      .text("Number of topics to generate")

    opt[Int]('i', "iterations")
      .valueName("<value>")
      .action( (x, c) => c.copy(maxIterations = x) )
      .text("Maximum number of iterations to run")

    opt[Int]("mintf")
      .valueName("<value>")
      .action( (x, c) => c.copy(minTF = x) )
      .text("Minimum term frequency required for a token in a single document")

    opt[Int]("mindf")
      .valueName("<value>")
      .action( (x, c) => c.copy(minDF = x) )
      .text("Minimum document frequency required for a token in the whole corpus")

  }

  def textToVector(
                    documents : RDD[(Long,String)],
                    stopwords : Array[String],
                    sc : SparkContext
                  ) : DataFrame = {

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val docDF = documents.toDF("docId", "text")

    // Split each document into words
    val tokens = new RegexTokenizer()
      .setGaps(false) // tells the tokenizer to look for tokens matching the pattern rather than finding whitespace
      .setPattern("#?\\p{L}{4,}+")
      .setInputCol("text")
      .setOutputCol("words")
      .transform(docDF)

    // Filter out stopwords
    val filteredTokens = new StopWordsRemover()
      .setStopWords(stopwords)
      .setCaseSensitive(false)
      .setInputCol("words")
      .setOutputCol("filtered")
      .transform(tokens)

    filteredTokens
  }

  /**
    * Count up activities
    *
    * @param numTopics Topic count
    * @param documents RDD of tweets
    * @param stopwords List of stopwords to ignore
    * @param sc Spark context, needed for SQL context creation
    * @param maxIterations Maximum number of iterations to perform
    * @param minTF Minimum frequency a token must have in a document to be included in that document
    * @param minDF Minimum document frequency a token must have to be included
    */
  def runLda(numTopics : Int,
             documents : RDD[(Long,String)],
             stopwords : Array[String],
             sc : SparkContext,
             maxIterations : Int,
             minTF : Int,
             minDF : Int
            ) : (LDAModel, CountVectorizerModel) = {

    val filteredTokens = textToVector(documents, stopwords, sc)

    // Limit to top `vocabSize` most common words and convert to word count vector features
    val cvModel = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")
      .setMinTF(minTF)
      .setMinDF(minDF)
      .fit(filteredTokens)

    val countVectors = cvModel.transform(filteredTokens)
      .select("docId", "features")
      .rdd
      .map { case Row(docId: Long, features: MLVector) => (docId, Vectors.fromML(features)) }
      .cache()

    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer())
      .setK(numTopics)
      .setMaxIterations(maxIterations)
      .setDocConcentration(-1) // use default symmetric document-topic prior
      .setTopicConcentration(-1) // use default symmetric topic-word prior

    val ldaModel = lda.run(countVectors)

    return (ldaModel, cvModel)
  }

  def printResults(ldaModel : LDAModel, cvModel : Array[String]) : Unit = {
    /**
      * Print results.
      */
    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (cvModel(term.toInt), weight) }
    }
    println(s"Topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }
  }

}
