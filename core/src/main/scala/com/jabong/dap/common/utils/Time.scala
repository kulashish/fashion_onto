package com.jabong.dap.common.utils

import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

import com.jabong.dap.common.Constants

/**
 * Created by Rachit on 28/5/15.
 */
object Time {

  def daysBetweenTwoDates(date1: Date, date2: Date): BigInt = {
    Math.abs(date1.getTime - date2.getTime) / Constants.ConvertMillisecToDays
  }

  def daysFromToday(date: Date): BigInt = {
    val today = new Date
    Math.abs(today.getTime - date.getTime) / Constants.ConvertMillisecToDays
  }

  def dateBefore30Days(): Date = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -30)
    cal.getTime
  }

  def getTodayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DateFormat)
    sdf.format(new Date())
  }

  def getYesterdayDate(): String = {
    val sdf = new SimpleDateFormat(Constants.DateFormat)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -1)
    sdf.format(cal.getTime)
  }

  def getTodayDateWithHrs(): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    sdf.format(new Date())
  }

  case class MonthYear(month: Int, year: Int, day: Int)

  def getMonthAndYear(dt: String): MonthYear = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DateFormat)
      val date = df.parse(dt)
      cal.setTime(date)
    }
    new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
  }

  def getMaxDaysOfMonth(dt: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(Constants.DateFormat)
      val date = df.parse(dt)
      cal.setTime(date)
    }
    cal.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

  def dateStringEmpty(dt: String): Boolean = {
    if (dt == null || dt.length() == 0)
      true
    else
      false
  }

  def isStrictlyLessThan(dt1: String, dt2: String): Boolean = {
    val format = new SimpleDateFormat(Constants.DateTimeFormat)
    try{
      val start = format.parse(dt1)
      val end = format.parse(dt2)
      if (start.getTime < end.getTime)
        true
      else
        false
    }
  }

  def isSameMonth(dt1: String, dt2: String) : Boolean = {
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

  def isSameDay(dt1: String, dt2: String) : Boolean = {
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
