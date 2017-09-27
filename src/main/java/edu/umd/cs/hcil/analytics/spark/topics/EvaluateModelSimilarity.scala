package edu.umd.cs.hcil.analytics.spark.topics

import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.twitter.TweetParser
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.{DenseMatrix, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 7/27/17.
  */
object EvaluateModelSimilarity {

  case class Config(inputLeftDataPath : String = "",
                    inputRightDataPath : String = "",
                    inputLeftVectorPath : String = "",
                    inputRightVectorPath : String = "",
                    numPartitions : Int = -1)

  val parser = new scopt.OptionParser[Config]("ModelEvaluator"){

    head("ModelSimilarityEvaluator", "1.0a")

    opt[String]('l', "left").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputLeftDataPath = x)} )
      .text("Input model for left topic set")

    opt[String]('r', "right").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputRightDataPath = x)} )
      .text("Input model for right topic set")

    opt[String]('x', "leftcv").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputLeftVectorPath = x)} )
      .text("Left count vectorizer")

    opt[String]('y', "rightcv").required()
      .valueName("<file>")
      .action( (x, c) => {c.copy(inputRightVectorPath = x)} )
      .text("Right count vectorizer")

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

    val leftDataPath = argConf.inputLeftDataPath
    val rightDataPath = argConf.inputRightDataPath

    val rightModel : LocalLDAModel = try {
      println("Attemping to load right distributed model...")
      val model = DistributedLDAModel.load(sc, rightDataPath)
      println("Distributed model loaded.")
      model.toLocal
    } catch {
      case e : Exception => {
        println("Attemping to load right LOCAL model...")
        val model = LocalLDAModel.load(sc, rightDataPath)
        println("right Local model loaded.")

        model
      }
    }
    val rightCvModel = CountVectorizerModel.load(argConf.inputRightVectorPath)

    val leftModel : LocalLDAModel = try {
      println("Attemping to left load distributed model...")
      val model = DistributedLDAModel.load(sc, leftDataPath)
      println("Distributed left model loaded.")
      model.toLocal
    } catch {
      case e : Exception => {
        println("Attemping to load left LOCAL model...")
        val model = LocalLDAModel.load(sc, leftDataPath)
        println("left Local model loaded.")

        model
      }
    }
    val leftCvModel = CountVectorizerModel.load(argConf.inputLeftVectorPath)

    // Now map Twitter's vocab to Reddit's vocab, so we can compute similarity
    val leftToRightVocabMap = leftCvModel.vocabulary.zipWithIndex.map(tup => {
      val token = tup._1
      val leftIndex = tup._2

      if ( rightCvModel.vocabulary.contains(token) ) {
        (leftIndex, rightCvModel.vocabulary.indexOf(token))
      } else {
        (leftIndex, -1)
      }
    }).toMap

    // Create the similarity matrix
    val similarityMatrixArray = leftModel.topicsMatrix.transpose.rowIter.map(row => {
      val rowMag = Vectors.norm(row, 2)

      val sims = rightModel.topicsMatrix.colIter.map(col => {
        val colMag = Vectors.norm(col, 2)
        var sum = 0.0d
        row.foreachActive((i, v) => {
          val redditIndex  = leftToRightVocabMap(i)
          if ( redditIndex > -1 ) {
            sum += col.apply(redditIndex) * v
          }
        })
        sum / (rowMag * colMag)
      })

      sims.toArray
    }).toArray

//    val similarityMatrix = new DenseMatrix(twitterModel.k, redditModel.k, similarityMatrixArray.reduce((l, r) => l ++ r), true)

    similarityMatrixArray.foreach(row => {
      row.foreach(e => print(e.toString + ",\t"))
      println()
    })
  }

}
