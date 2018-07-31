package edu.umd.cs.hcil.analytics.spark.temporal.reddit

import java.util.{Calendar, Date}

import edu.umd.cs.hcil.analytics.utils.TimeScale
import edu.umd.cs.hcil.models.fourchan.ThreadParser
import edu.umd.cs.hcil.models.fourchan.ThreadParser.ThreadModel
import edu.umd.cs.hcil.models.reddit.SubmissionParser
import edu.umd.cs.hcil.models.reddit.SubmissionParser.SubmissionModel
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
    val textFields : RDD[(String, SubmissionModel)] = messages.map(line => {
      (line, SubmissionParser.parseJson(line))
    }).filter(threadTup => threadTup._2 != null && (threadTup._2.created_utc != null || threadTup._2.created != null))

    val this_time_scale = TimeScale.DAILY

    val dates = textFields.map(threadTup => {
      val timeframe = this_time_scale
      val this_date = if ( threadTup._2.created_utc != null ) {
        new Date(threadTup._2.created_utc.get * 1000)
      } else {
        new Date(threadTup._2.created.get * 1000)
      }

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
      val this_date = if ( tup._2.created_utc != null ) {
        new Date(tup._2.created_utc.get * 1000)
      } else {
        new Date(tup._2.created.get * 1000)
      }
      val this_date_flat = TimeScale.convertTimeToSlice(this_date, this_time_scale)

      (this_date_flat, thread_json)
    })

    dated_json.cache()
    for ( today <- all_days ) {
      val this_day_json = dated_json.filter(tup => tup._1.compareTo(today) == 0).map(tup => tup._2)

      val this_calendar = Calendar.getInstance()
      this_calendar.setTime(today)
      val output_path = "%s/submissions-%04d-%02d-%02d.json.gz".format(outputPath,
        this_calendar.get(Calendar.YEAR),
        this_calendar.get(Calendar.MONTH) + 1,
        this_calendar.get(Calendar.DAY_OF_MONTH))
      println("Writing to: " + output_path)

      this_day_json.saveAsTextFile(output_path, classOf[org.apache.hadoop.io.compress.GzipCodec])
    }
  }

}
