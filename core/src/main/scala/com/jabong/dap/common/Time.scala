package com.jabong.dap.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.jabong.dap.common.Constants

import scala.collection.immutable.HashMap

/**
 * Created by jabong on 28/5/15.
 */
object Time {

  def daysFromToday(date: Date): BigInt = {
    val today = new Date
    daysBetweenTwoDates(today, date)
  }

  def daysBetweenTwoDates(date1: Date, date2: Date): BigInt = {
    Math.abs(date1.getTime - date2.getTime) / Constants.CONVERT_MILLISEC_TO_DAYS
  }

  def dateBeforeDays(n: Int, dateFormat: String): String = {
    val sdf = new SimpleDateFormat(dateFormat)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, n)
    sdf.format(cal.getTime())
  }

  def getTimeStamp(date: String, dateFormat: String): Timestamp = {
    val sdf = new SimpleDateFormat(dateFormat)
    val dt = sdf.parse(date)
    val time = dt.getTime()
    val ts = new Timestamp(time)
    ts
  }

  def getTodayDate(dateFormat: String): String = {
    val sdf = new SimpleDateFormat(dateFormat)
    sdf.format(new Date())
  }

  def getMonthAndYear(dt: String, dateFormat: String): MonthYear = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val sdf = new SimpleDateFormat(dateFormat)
      var date = sdf.parse(dt)
      cal.setTime(date)
    }
    val my = new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
    my
  }

  def getMaxDaysOfMonth(dt: String, dateFormat: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val sdf = new SimpleDateFormat(dateFormat)
      var date = sdf.parse(dt)
      cal.setTime(date)
    }
    cal.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

  def timeToSlot(dateString: String, dateFormat: String): Int = {

    var timeToSlotMap = new HashMap[Int, Int]
    timeToSlotMap +=(7 -> 1, 8 -> 1, 9 -> 2, 10 -> 2, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4
      , 15 -> 5, 16 -> 5, 17 -> 6, 18 -> 6, 19 -> 7, 20 -> 7, 21 -> 8, 22 -> 8, 23 -> 9, 0 -> 9, 1 -> 10
      , 2 -> 10, 3 -> 11, 4 -> 11, 5 -> 12, 6 -> 12)

    val formatter = new SimpleDateFormat(dateFormat)
    var date: java.util.Date = null
    date = formatter.parse(dateString)

    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    val hours = calendar.get(Calendar.HOUR_OF_DAY)
    val timeSlot = timeToSlotMap.getOrElse(hours, 0)
    timeSlot
  }

  case class MonthYear(val month: Int, val year: Int, val day: Int)

}