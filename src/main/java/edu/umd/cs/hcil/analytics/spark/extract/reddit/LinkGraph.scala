package edu.umd.cs.hcil.analytics.spark.extract.reddit

import scala.collection.JavaConverters._
import edu.umd.cs.hcil.analytics.utils.UrlDomainExtractor
import edu.umd.cs.hcil.models.reddit.{CommentParser, SubmissionParser}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.nibor.autolink.{LinkExtractor,LinkType,LinkSpan}

/**
  * Created by cbuntain on 4/26/18.
  */
object LinkGraph {

  @transient lazy val linkExtractor = LinkExtractor.builder()
    .linkTypes(Set(LinkType.URL, LinkType.WWW).asJava)
    .build()

  /**
    * @param args the command line arguments
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hyperlink User To Domain [Reddit]")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    val messages : RDD[String] = if ( args.size > 3 ) {
      val initialPartitions = args(3).toInt
      println("New Partition Count: " + initialPartitions)

      messagesRaw.repartition(initialPartitions)
    } else {
      messagesRaw
    }

    val edge_tuples : RDD[(String, String, Long)] = messages.flatMap(itemStr => {
      if ( itemStr.indexOf("\"selftext\"") > -1 ) {
        val submission = SubmissionParser.parseJson(itemStr)

        if (submission != null &&
          submission.author != null && !submission.author.equalsIgnoreCase("[deleted]") &&
          submission.url != null && submission.url.length > 0
        ) {
          val userId = submission.author

          val timestamp: Long = if (submission.created_utc.isDefined) {
            submission.created_utc.get
          } else if (submission.created.isDefined) {
            submission.created.get
          } else {
            -1
          }

          val url = submission.url
          val tld = UrlDomainExtractor.getTLD(url)

          if (tld.isDefined) {
            List((userId, tld.get.toLowerCase, timestamp))
          } else {
            List.empty
          }

        } else {
          List.empty
        }
      } else if ( itemStr.indexOf("\"parent_id\"") > -1 ) {

        val comment = CommentParser.parseJson(itemStr)

        if ( comment != null &&
          comment.author != null && !comment.author.equalsIgnoreCase("[deleted]") &&
          comment.body != null && comment.body.length > 0
        ) {

          val userId = comment.author

          val timestamp: Long = if (comment.created_utc.isDefined) {
            comment.created_utc.get
          } else if (comment.created.isDefined) {
            comment.created.get
          } else {
            -1
          }

          // parse body to find a link
          val domains = linkExtractor.extractLinks(comment.body).iterator().asScala.map(link => {
            val url = comment.body.substring(link.getBeginIndex, link.getEndIndex)

            if ( !url.startsWith("http") ) {
              UrlDomainExtractor.getTLD("http://" + url)
            } else {
              UrlDomainExtractor.getTLD(url)
            }

          }).filter(url => url.isDefined).map(url => url.get).map(url => (userId, url.toLowerCase, timestamp))

          domains

        } else {
          List.empty
        }
      } else {
        List.empty
      }
    }).filter(tup => tup != null)

    val edges = edge_tuples.map(tup => "%s\t%s\t{\"time\":%d}".format(tup._1, tup._2, tup._3))

    edges.saveAsTextFile(outputPath, classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

}
