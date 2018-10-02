package edu.umd.cs.hcil.analytics.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

/**
  * Created by cbuntain on 7/20/17.
  */
object TimeScale extends Enumeration {
  type TimeScale = Value
  val MINUTE, HOURLY, DAILY = Value

  /**
    * Flatten timestamp to the appropriate scale
    *
    * @param time The date to flatten
    * @param scale The scale we're using
    */
  def convertTimeToSlice(time : Date, scale : TimeScale.Value) : Date = {
    val cal = Calendar.getInstance
    cal.setTime(time)

    if ( scale == TimeScale.MINUTE || scale == TimeScale.HOURLY || scale == TimeScale.DAILY ) {
      cal.set(Calendar.SECOND, 0)
    }

    if ( scale == TimeScale.HOURLY || scale == TimeScale.DAILY ) {
      cal.set(Calendar.MINUTE, 0)
    }

    if ( scale == TimeScale.DAILY ) {
      cal.set(Calendar.HOUR_OF_DAY, 0)
    }

    return cal.getTime
  }

  /**
    * Build the full date list between start and end
    *
    * @param startDate The first date in the list
    * @param endDate The last date in the list
    * @param scale The scale on which we are counting (minutes, hours, days)
    */
  def constructDateList(startDate : Date, endDate : Date, scale : TimeScale.Value) : List[Date] = {
    val cal = Calendar.getInstance
    cal.setTime(startDate)

    var l = List[Date]()

    while(cal.getTime.before(endDate)) {
      l = l :+ cal.getTime

      if ( scale == TimeScale.MINUTE ) {
        cal.add(Calendar.MINUTE, 1)
      } else if ( scale == TimeScale.HOURLY ) {
        cal.add(Calendar.HOUR, 1)
      } else if ( scale == TimeScale.DAILY ) {
        cal.add(Calendar.DATE, 1)
      }
    }
    l = l :+ endDate

    return l
  }

  /**
    * Convert a string switch to the corresponding scale value
    *
    * @param timeScaleStr The string argument to convert
    *
    */
  def switchToScale(timeScaleStr : String) : Option[TimeScale.Value] = {
    // Validate and set time scale
    val timeScale : Option[TimeScale.Value] = if ( timeScaleStr == "-m" ) {
      Some(TimeScale.MINUTE)
    } else if ( timeScaleStr == "-h" ) {
      Some(TimeScale.HOURLY)
    } else if ( timeScaleStr == "-d" ) {
      Some(TimeScale.DAILY)
    } else {
      None
    }

    return timeScale
  }

  // Twitter's time format'
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
