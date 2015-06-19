package com.jabong.dap.common.utils

import java.text.SimpleDateFormat

import com.jabong.dap.common.Constants
import com.jabong.dap.common.utils.Time.MonthYear
import org.scalatest.{Matchers, FlatSpec}
import java.util.{Calendar, Date}

/**
 * Created by Rachit on 19/6/15.
 */
class TimeTest extends FlatSpec with Matchers {
  val format = new SimpleDateFormat(Constants.DateTimeFormat)
  val date1 = format.parse("2015-06-19 18:00:00")
  val date2 = format.parse("2015-05-19 18:00:00")
  val today = new Date
  val numDaysFromToday = Math.abs(today.getTime - date2.getTime) / Constants.ConvertMillisecToDays

  "daysBetweenTwoDates" should "return 31" in {
    Time.daysBetweenTwoDates(date1, date2) should be (31)
  }

  "daysFromToday" should "return " + numDaysFromToday in {
    Time.daysFromToday(date2) should be (numDaysFromToday)
  }

  "dateBefore30Days" should "return correct value" in {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, -30)
    Time.dateBefore30Days() should be (cal.getTime)
  }

  "getTodayDate" should "return correct value" in {
    val sdf = new SimpleDateFormat(Constants.DateFormat)
    Time.getTodayDate() should be (sdf.format(today))
  }

  "getYesterdayDate" should "return correct value" in {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat(Constants.DateFormat)
    cal.setTime(today)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    Time.getYesterdayDate() should be (sdf.format(cal.getTime))
  }

  "getTodayDateWithHrs" should "return correct value" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    Time.getTodayDateWithHrs() should be (sdf.format(today))
  }

  "getMonthAndYear" should "return correct value" in {
    val my = new MonthYear(6, 2015, 19)
    Time.getMonthAndYear("2015-06-19") should be (my)
  }

  "getMaxDaysOfMonth" should "return 30" in {
    Time.getMaxDaysOfMonth("2015-06-19") should be (30)
  }

  "dateStringEmpty" should "return true" in {
    Time.dateStringEmpty("") should be (true)
  }

  "dateStringEmpty" should "return false" in {
    Time.dateStringEmpty("2015-06-19") should be (false)
  }

  "isStrictlyLessThan" should "return true" in {
    Time.isStrictlyLessThan("2015-05-19 00:00:00", "2015-06-19 00:00:00") should be (true)
  }

  "isStrictlyLessThan" should "return false" in {
    Time.isStrictlyLessThan("2015-07-19 00:00:00", "2015-06-19 00:00:00") should be (false)
  }

  "isSameMonth" should "return true" in {
    Time.isSameMonth("2015-06-19 00:00:00", "2015-06-20 00:00:00") should be (true)
  }

  "isSameMonth" should "return false" in {
    Time.isSameMonth("2015-06-19 00:00:00", "2015-05-19 00:00:00") should be (false)
  }

  "isSameDay" should "return true" in {
    Time.isSameDay("2015-06-19 00:00:00", "2015-06-19 00:00:00") should be (true)
  }

  "isSameDay" should "return false" in {
    Time.isSameDay("2015-06-19 00:00:00", "2015-05-19 00:00:00") should be (false)
  }
}
