package com.jabong.dap.common.time

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }
import scala.collection.immutable.HashMap

/**
 * Created by Rachit on 28/5/15.
 */
object TimeUtils {

  /**
   * Returns the total number of days between two given date inputs
   * @param date1
   * @param date2
   * @return
   */
  def daysBetweenTwoDates(date1: Date, date2: Date): BigInt = {
    Math.abs(date1.getTime - date2.getTime) / Constants.CONVERT_MILLISECOND_TO_DAYS
  }

  /**
   * Given a date input, returns the number of days between that date and today's date
   * @param date
   * @return
   */
  def daysFromToday(date: Date): BigInt = {
    val today = new Date
    daysBetweenTwoDates(today, date)
  }

  /**
   * Boolean test to check whether a given date string is empty (returns true) or not (returns false).
   * @param dt
   * @return
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
   * @param dt1
   * @param dt2
   * @return
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
   * Given two input date strings in the format yyyy-MM-dd, tells whether both lie in the same year or not.
   * WARNING: can raise ParseException if input dates not in the correct format.
   * @param dt1
   * @param dt2
   * @return
   */
  def isSameYear(dt1: String, dt2: String): Boolean = {
    val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
    val start = Calendar.getInstance()
    val end = Calendar.getInstance()
    start.setTime(format.parse(dt1))
    end.setTime(format.parse(dt2))
    if (start.get(Calendar.YEAR) == end.get(Calendar.YEAR))
      true
    else
      false
  }

  /**
   * Given two input date strings in the format yyyy-MM-dd, tells whether both lie in the same month of the same
   * year or not.
   * WARNING: can raise ParseException if input dates not in the correct format.
   * @param dt1
   * @param dt2
   * @return
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
   * @param dt1
   * @param dt2
   * @return
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

  /**
   * Returns the Date as a string in the given Date Format which is given no. of days after given input date.
   * If input date is null then use today's date.
   * If n is negative then returns the date as a string which is given no. of days before today's date.
   * @param noOfDays
   * @param dateFormat
   * @param date
   * @return
   */
  def getDateAfterNDays(noOfDays: Int, dateFormat: String, date: String): String = {
    val sdf = new SimpleDateFormat(dateFormat)
    val cal = Calendar.getInstance()
    if (date != null)
      cal.setTime(sdf.parse(date))
    cal.add(Calendar.DAY_OF_MONTH, noOfDays)
    sdf.format(cal.getTime())
  }

  /**
   * Returns the Date as a string in the given Date Format which is given no. of days after today's date.
   *   If n is negative then returns the date as a string which is given no. of days before today's date.
   * @param noOfDays
   * @param dateFormat
   * @return
   */
  def getDateAfterNDays(noOfDays: Int, dateFormat: String): String = {
    getDateAfterNDays(noOfDays, dateFormat, getTodayDate(dateFormat))
  }

  /**
   *
   * @param date
   * @param dateFormat
   * @return
   */
  def getTimeStamp(date: String, dateFormat: String): Timestamp = {
    val sdf = new SimpleDateFormat(dateFormat)
    val dt = sdf.parse(date)
    val time = dt.getTime()
    val ts = new Timestamp(time)
    ts
  }

  /**
   * Return today's date as a string in the given date format.
   * @param dateFormat
   * @return
   */
  def getTodayDate(dateFormat: String): String = {
    val sdf = new SimpleDateFormat(dateFormat)
    sdf.format(new Date())
  }

  /**
   *
   * @param dt
   * @param dateFormat
   * @return
   */
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

  /**
   *
   * @param dt
   * @param dateFormat
   * @return
   */
  def getMaxDaysOfMonth(dt: String, dateFormat: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val sdf = new SimpleDateFormat(dateFormat)
      val date = sdf.parse(dt)
      cal.setTime(date)
    }
    cal.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

  /**
   *
   * @param dateString
   * @param dateFormat
   * @return Int
   */
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

  /**
   * calculate total number of years from given date till today.
   * @param date
   * @return Int
   */

  def getYearFromToday(date: Date): Int = {

    if (date == null)
      return 0

    val cal = Calendar.getInstance()
    cal.setTime(date)

    val todayCal = Calendar.getInstance()

    return todayCal.get(Calendar.YEAR) - cal.get(Calendar.YEAR)

  }

  case class MonthYear(val month: Int, val year: Int, val day: Int)
}