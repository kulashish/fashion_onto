package com.jabong.dap.common.utils

import java.text.SimpleDateFormat

import com.jabong.dap.common.Constants
import org.scalatest.{Matchers, FlatSpec}
import java.util.Date

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
}
