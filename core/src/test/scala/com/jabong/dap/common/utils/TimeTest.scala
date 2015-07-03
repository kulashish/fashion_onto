package com.jabong.dap.common.utils

import java.text.SimpleDateFormat

import com.jabong.dap.common.Constants
import org.scalatest.{ Matchers, FlatSpec }
import java.util.{ Calendar, Date }

/**
 * Created by Rachit on 19/6/15.
 */
class TimeTest extends FlatSpec with Matchers {
  val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
  val date1 = format.parse("2015-06-19 18:00:00")
  val date2 = format.parse("2015-05-19 18:00:00")
  val today = new Date
  val numDaysFromToday = Math.abs(today.getTime - date2.getTime) / Constants.CONVERT_MILLISECOND_TO_DAYS

  "daysBetweenTwoDates" should "return 31" in {
    Time.daysBetweenTwoDates(date1, date2) should be (31)
  }

  "daysFromToday" should "return " + numDaysFromToday in {
    Time.daysFromToday(date2) should be (numDaysFromToday)
  }

  "getTodayDate" should "return correct value" in {
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    Time.getTodayDate() should be (sdf.format(today))
  }

  "getYesterdayDate" should "return correct value" in {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    cal.setTime(today)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    Time.getYesterdayDate() should be (sdf.format(cal.getTime))
  }

  "getYesterdayDate" should "return correct value on an input date" in {
    val inputDate = "2015-06-01"
    val outputDate = "2015-05-31"
    Time.getYesterdayDate(inputDate) should be (outputDate)
  }

  "getDayBeforeYesterdayDate" should "return correct value" in {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT)
    cal.setTime(today)
    cal.add(Calendar.DAY_OF_MONTH, -2)
    Time.getDayBeforeYesterdayDate() should be (sdf.format(cal.getTime))
  }

  "getTodayDateWithHrs" should "return correct value" in {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    Time.getTodayDateWithHrs() should be (sdf.format(today))
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
