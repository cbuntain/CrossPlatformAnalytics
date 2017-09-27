package edu.umd.cs.hcil.analytics.spark.temporal.fourchan

import java.util.{Calendar, Date}

import edu.umd.cs.hcil.analytics.utils.TimeScale
import edu.umd.cs.hcil.models.fourchan.ThreadParser
import edu.umd.cs.hcil.models.fourchan.ThreadParser.ThreadModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cbuntain on 9/26/17.
  */
object TemporalDivider {

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("4chan Daily Splitter")
    val sc = new SparkContext(conf)

    val dataPath = args(0)
    val outputPath = args(1)

    val messagesRaw = sc.textFile(dataPath)
    println("Initial Partition Count: " + messagesRaw.partitions.size)

    var messages = messagesRaw
    if ( args.size > 2 ) {
      val initialPartitions = args(2).toInt
      messages = messagesRaw.repartition(initialPartitions)
      println("New Partition Count: " + messages.partitions.size)
    }

    // Convert each JSON line in the file to a submission
    val textFields : RDD[(String, ThreadModel)] = messages.map(line => {
      (line, ThreadParser.parseJson(line))
    }).filter(threadTup => threadTup._2 != null && threadTup._2.posts != null && threadTup._2.posts.head != null)

    val this_time_scale = TimeScale.DAILY

    val dates = textFields.map(threadTup => {
      val timeframe = this_time_scale
      val this_date = new Date(threadTup._2.posts.head.time * 1000)

      (TimeScale.convertTimeToSlice(this_date, timeframe), 1)
    }).reduceByKey(_ + _).collect.sortBy(t => t._1)

    val start_date = dates.head._1
    val end_date = dates.last._1

    val all_days = TimeScale.constructDateList(start_date, end_date, this_time_scale)
    val daily_count = dates.toMap
    for ( day <- all_days ) {
      println(day.toString() + " - " + daily_count.getOrElse(day, 0))
    }

    val dated_json = textFields.map(tup => {
      val thread_json = tup._1
      val first_post = tup._2.posts.head
      val this_date = TimeScale.convertTimeToSlice(new Date(first_post.time * 1000), this_time_scale)

      (this_date, thread_json)
    })

    dated_json.cache()
    for ( today <- all_days ) {
      val this_day_json = dated_json.filter(tup => tup._1.compareTo(today) == 0).map(tup => tup._2)

      val this_calendar = Calendar.getInstance()
      this_calendar.setTime(today)
      val output_path = outputPath + "/threads-" +
        this_calendar.get(Calendar.YEAR) + "-" +
        this_calendar.get(Calendar.MONTH) + "-" +
        this_calendar.get(Calendar.DAY_OF_MONTH) + ".json.gz"
      println("Writing to: " + output_path)

      this_day_json.saveAsTextFile(output_path, classOf[org.apache.hadoop.io.compress.GzipCodec])
    }
  }

}
