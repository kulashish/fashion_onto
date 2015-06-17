package com.jabong.dap.common

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.immutable.HashMap


/**
 * Created by jabong on 28/5/15.
 */
object Time {

  def daysBetweenTwoDates( date1:Date, date2:Date ) : BigInt = {
      return Math.abs(date1.getTime-date2.getTime)/Constants.CONVERT_MILLISEC_TO_DAYS
  }


  def daysFromToday(date : Date): BigInt = {
    val today = new Date
    return Math.abs(today.getTime-date.getTime)/Constants.CONVERT_MILLISEC_TO_DAYS
  }

  def dateBeforeDays(n :Int): Date = {
    val format = new SimpleDateFormat(Constants.DATETIME_FORMAT)
    val cal=Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, n)
    return cal.getTime
  }

  def getTimeStamp(date: String): Timestamp={
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
    val dat = dateFormat.parse(date)
    val time = dat.getTime()
    val ts= new Timestamp(time)
    return ts
  }

  def getTodayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    return sdf.format(new Date())
  }

  def getYesterdayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    val cal=Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -1)
    return sdf.format(cal.getTime())
  }

  def getTodayDateWithHrs(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    return sdf.format(new Date())
  }

  case class MonthYear(val month: Int, val year: Int, val day: Int)

  def getMonthAndYear(dt : String): MonthYear = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DATE_FORMAT)
      var date = df.parse(dt)
      cal.setTime(date)
    }
    val my = new MonthYear(cal.get(Calendar.MONTH),cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
    return my
  }

  def getYear(dt : String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DATE_FORMAT)
      var date = df.parse(dt)
      cal.setTime(date)
    }
    return cal.get(Calendar.YEAR)
  }

  def getMaxDaysOfMonth(dt: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DATE_FORMAT)
      var date = df.parse(dt)
      cal.setTime(date)
    }
    return cal.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

  def timeToSlot(dateString: String): Int = {

    var timeToSlotMap = new HashMap[Int, Int]
    timeToSlotMap +=(7 -> 1, 8 -> 1, 9 -> 2, 10 -> 2, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4
      , 15 -> 5, 16 -> 5, 17 -> 6, 18 -> 6, 19 -> 7, 20 -> 7, 21 -> 8, 22 -> 8, 23 -> 9, 0 -> 9, 1 -> 10
      , 2 -> 10, 3 -> 11, 4 -> 11, 5 -> 12, 6 -> 12)

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    var date : java.util.Date=null
    date = formatter.parse(dateString);

    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val hours = calendar.get(Calendar.HOUR_OF_DAY);
    val timeSlot = timeToSlotMap.getOrElse(hours, 0)
    return timeSlot
  }

}
