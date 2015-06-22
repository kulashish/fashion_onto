package com.jabong.dap.common.utils

import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

import com.jabong.dap.common.Constants

/**
 * Created by Rachit on 28/5/15.
 */
object Time {

  /**
   * Returns the total number of days between two given date inputs
   */
  def daysBetweenTwoDates(date1: Date, date2: Date): BigInt = {
    Math.abs(date1.getTime - date2.getTime) / Constants.ConvertMillisecToDays
  }

  /**
   * Given a date input, returns the number of days between that date and today's date
   */
  def daysFromToday(date: Date): BigInt = {
    val today = new Date
    Math.abs(today.getTime - date.getTime) / Constants.ConvertMillisecToDays
  }

  /**
   * Returns the date 30 days back from today's date
   */
  def dateBefore30Days(): Date = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -30)
    cal.getTime
  }
  /**
   * Returns today's date as a string in the format yyyy-MM-dd.
   */
  def getTodayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DateFormat)
    sdf.format(new Date())
  }

  /**
   * Returns yesterday's date as a string in the format yyyy-MM-dd
   */
  def getYesterdayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DateFormat)
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

  case class MonthYear(month: Int, year: Int, day: Int)

  /**
   * Returns an instance of MonthYear for the date input.
   */
  def getMonthAndYear(dt: String): MonthYear = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DateFormat)
      val date = df.parse(dt)
      cal.setTime(date)
    }
    // Uses an index that starts from 0 for month. Hence need to add 1 to the month.
    new MonthYear(cal.get(Calendar.MONTH)+1, cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
  }

  /**
   * Returns the maximum date of the month for the inputted date.
   * e.g. for input 2015-2-13, the output would be 28.
   */
  def getMaxDaysOfMonth(dt: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DateFormat)
      val date = df.parse(dt)
      cal.setTime(date)
    }
    cal.getActualMaximum(Calendar.DAY_OF_MONTH)
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
    val format = new SimpleDateFormat(Constants.DateTimeFormat)
    try {
      val start = format.parse(dt1)
      val end = format.parse(dt2)
      if (start.getTime < end.getTime)
        true
      else
        false
    }
  }

  /**
   * Given two input date strings in the format yyyy-MM-dd, tells whether both lie in the same month of the same
   * year or not.
   * WARNING: can raise ParseException if input dates not in the correct format.
   */
  def isSameMonth(dt1: String, dt2: String): Boolean = {
    val format = new SimpleDateFormat(Constants.DateTimeFormat)
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
    val format = new SimpleDateFormat(Constants.DateTimeFormat)
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

}
