package edu.umd.cs.hcil.analytics.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
  * Created by cbuntain on 7/20/17.
  */
object DateFormatter {
  // Output time format'
  def TIME_FORMAT = "EEE MMM d HH:mm:ss Z yyyy"

  /**
    * Convert a given date into a string using TIME_FORMAT
    *
    * @param date The date to convert
    */
  def dateFormatter(date : Date) : String = {
    val sdf = new SimpleDateFormat(TIME_FORMAT, Locale.US)
    sdf.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))

    sdf.format(date)
  }
}
