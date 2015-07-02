package com.jabong.dap.common.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

import com.jabong.dap.common.Constants

import scala.collection.immutable.HashMap

/**
 * Created by Rachit on 28/5/15.
 */
object Time {

  /**
   * Returns the total number of days between two given date inputs
   */
  def daysBetweenTwoDates(date1: Date, date2: Date): BigInt = {
    Math.abs(date1.getTime - date2.getTime) / Constants.CONVERT_MILLISECOND_TO_DAYS
  }

  /**
   * Given a date input, returns the number of days between that date and today's date
   */
  def daysFromToday(date: Date): BigInt = {
    val today = new Date
    Math.abs(today.getTime - date.getTime) / Constants.CONVERT_MILLISECOND_TO_DAYS
  }

  /**
   * Returns today's date as a string in the format yyyy-MM-dd.
   */
  def getTodayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    sdf.format(new Date())
  }

  /**
   * Returns yesterday's date as a string in the format yyyy-MM-dd
   */
  def getYesterdayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -1)
    sdf.format(cal.getTime)
  }

  /**
   * Returns today's date as a string in the format yyyy-MM-dd--HH.
   */
  def getTodayDateWithHrs(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    sdf.format(new Date())
  }

  /**
   * Boolean test to check whether a given date string is empty (returns true) or not (returns false).
   */
  def dateStringEmpty(dt: String): Boolean = {
    if (dt == null || dt.length() == 0)
      true
    else
      false
  }

  /**
   * Given two input date strings in the format yyyy-MM-dd, tells whether the first date is less than the second
   * date or not.
   * WARNING: can raise ParseException if input dates not in the correct format.
   */
  def isStrictlyLessThan(dt1: String, dt2: String): Boolean = {
    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
    val start = format.parse(dt1)
    val end = format.parse(dt2)
    if (start.getTime < end.getTime)
      true
    else
      false
  }

  /**
   * Given two input date strings in the format yyyy-MM-dd, tells whether both lie in the same month of the same
   * year or not.
   * WARNING: can raise ParseException if input dates not in the correct format.
   */
  def isSameMonth(dt1: String, dt2: String): Boolean = {
    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
    val start = Calendar.getInstance()
    val end = Calendar.getInstance()
    start.setTime(format.parse(dt1))
    end.setTime(format.parse(dt2))
    if ((start.get(Calendar.YEAR) == end.get(Calendar.YEAR)) && (start.get(Calendar.MONTH) == end.get(Calendar.MONTH)))
      true
    else
      false
  }

  /**
   * Given two input date strings in the format yyyy-MM-dd, tells whether both lie in the same day of the same
   * month of the same year or not.
   * WARNING: can raise ParseException if input dates not in the correct format.
   */
  def isSameDay(dt1: String, dt2: String): Boolean = {
    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
    val start = Calendar.getInstance()
    val end = Calendar.getInstance()
    start.setTime(format.parse(dt1))
    end.setTime(format.parse(dt2))
    if ((start.get(Calendar.YEAR) == end.get(Calendar.YEAR)) && (start.get(Calendar.MONTH) == end.get(Calendar.MONTH))
      && (start.get(Calendar.DATE) == end.get(Calendar.DATE)))
      true
    else
      false
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
      val date = sdf.parse(dt)
      cal.setTime(date)
    }
    val my = new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
    my
  }

  def getMaxDaysOfMonth(dt: String, dateFormat: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val sdf = new SimpleDateFormat(dateFormat)
      val date = sdf.parse(dt)
      cal.setTime(date)
    }
    cal.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

  def timeToSlot(dateString: String, dateFormat: String): Int = {

    var timeToSlotMap = new HashMap[Int, Int]
    timeToSlotMap += (7 -> 1, 8 -> 1, 9 -> 2, 10 -> 2, 11 -> 3, 12 -> 3, 13 -> 4, 14 -> 4, 15 -> 5, 16 -> 5, 17 -> 6, 18 -> 6, 19 -> 7, 20 -> 7, 21 -> 8, 22 -> 8, 23 -> 9, 0 -> 9, 1 -> 10, 2 -> 10, 3 -> 11, 4 -> 11, 5 -> 12, 6 -> 12)

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