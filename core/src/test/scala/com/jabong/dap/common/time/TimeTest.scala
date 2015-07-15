package com.jabong.dap.common.time

import java.text.SimpleDateFormat

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
    TimeUtils.daysBetweenTwoDates(date1, date2) should be (31)
  }

  "daysFromToday" should "return " + numDaysFromToday in {
    TimeUtils.daysFromToday(date2) should be (numDaysFromToday)
  }

  "getTodayDate1" should "return correct value" in {
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT_FOLDER)
    TimeUtils.getTodayDate(Constants.DATE_FORMAT_FOLDER) should be (sdf.format(today))
  }

  "getDateAfterNDays" should "return correct value" in {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat(Constants.DATE_FORMAT_FOLDER)
    cal.setTime(today)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER) should be (sdf.format(cal.getTime))
  }

  "getTodayDate2" should "return correct value" in {
    val sdf = new SimpleDateFormat(Constants.DATE_TIME_FORMAT_HRS_FOLDER)
    TimeUtils.getTodayDate(Constants.DATE_TIME_FORMAT_HRS_FOLDER) should be (sdf.format(today))
  }

  "isStrictlyLessThan" should "return true" in {
    TimeUtils.isStrictlyLessThan("2015-05-19 00:00:00", "2015-06-19 00:00:00") should be (true)
  }

  "isStrictlyLessThan" should "return false" in {
    TimeUtils.isStrictlyLessThan("2015-07-19 00:00:00", "2015-06-19 00:00:00") should be (false)
  }

  "isSameMonth" should "return true" in {
    TimeUtils.isSameMonth("2015-06-19 00:00:00", "2015-06-20 00:00:00") should be (true)
  }

  "isSameMonth" should "return false" in {
    TimeUtils.isSameMonth("2015-06-19 00:00:00", "2015-05-19 00:00:00") should be (false)
  }

  "isSameDay" should "return true" in {
    TimeUtils.isSameDay("2015-06-19 00:00:00", "2015-06-19 00:00:00") should be (true)
  }

  "isSameDay" should "return false" in {
    TimeUtils.isSameDay("2015-06-19 00:00:00", "2015-05-19 00:00:00") should be (false)
  }
}