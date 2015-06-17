package com.jabong.dap.common.utils

import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

import com.jabong.dap.common.{ AppConfig }

object Time {

  def daysBetweenTwoDates(date1: Date, date2: Date): BigInt = {
    Math.abs(date1.getTime - date2.getTime) / AppConfig.OneDayMilliSeconds
  }

  def daysFromToday(date: Date): BigInt = {
    val today = new Date
    Math.abs(today.getTime - date.getTime) / AppConfig.OneDayMilliSeconds
  }

  def dateBefore30Days(): Date = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -30)
    cal.getTime
  }

  def getTodayDate(): String = {
    val sdf = new SimpleDateFormat(AppConfig.DateFormat)
    sdf.format(new Date())
  }

  def getYesterdayDate(): String = {
    val sdf = new SimpleDateFormat(AppConfig.DateFormat)
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
      val df = new SimpleDateFormat(AppConfig.DateFormat)
      val date = df.parse(dt)
      cal.setTime(date)
    }
    new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
  }

  def getMaxDaysOfMonth(dt: String): Int = {
    val cal = Calendar.getInstance()
    if (null != dt) {
      val df = new SimpleDateFormat(AppConfig.DateFormat)
      val date = df.parse(dt)
      cal.setTime(date)
    }
    cal.getActualMaximum(Calendar.DAY_OF_MONTH)
  }

}