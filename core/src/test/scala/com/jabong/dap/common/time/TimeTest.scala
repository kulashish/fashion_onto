package com.jabong.dap.common.time

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.jabong.dap.common.time.TimeUtils.MonthYear
import org.scalatest.{ Matchers, FlatSpec }
import java.util.{ Calendar, Date }

/**
 * Created by Rachit on 19/6/15.
 */
class TimeTest extends FlatSpec with Matchers {
  val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
  val date1 = format.parse("2015-06-19 18:00:00")
  val date2 = format.parse("2015-05-19 18:00:00")
  val today = new Date
  val numDaysFromToday = (Math.abs(today.getTime - date2.getTime) / TimeConstants.CONVERT_MILLISECOND_TO_DAYS).toInt

  val calendar = Calendar.getInstance()
  calendar.add(Calendar.DAY_OF_MONTH, -1)
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
  val testDate = Timestamp.valueOf(dateFormat.format(calendar.getTime))

  val TRUE = true
  val FALSE = false

  "withLeadingZeros" should "add a zero if input is less than 10" in {
    val input = 7
    val output = "07"
    TimeUtils.withLeadingZeros(input) should be (output)
  }

  "withLeadingZeros" should "not add a zero if input is greater than 9" in {
    val input = 23
    val output = "23"
    TimeUtils.withLeadingZeros(input) should be (output)
  }

  "daysBetweenTwoDates" should "return 31" in {
    TimeUtils.daysBetweenTwoDates(date1, date2) should be (31)
  }

  "daysFromToday" should "return " + numDaysFromToday in {
    TimeUtils.daysFromToday(date2) should be (numDaysFromToday)
  }

  "getTodayDate1" should "return correct value" in {
    val sdf = new SimpleDateFormat(TimeConstants.DATE_FORMAT_FOLDER)
    TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER) should be (sdf.format(today))
  }

  "getDateAfterNDays" should "return correct value" in {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat(TimeConstants.DATE_FORMAT_FOLDER)
    cal.setTime(today)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER) should be (sdf.format(cal.getTime))
  }

  "getTodayDate2" should "return correct value" in {
    val sdf = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)
    TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER) should be (sdf.format(today))
  }

  "Yesterdays date " should "return 1 in day diff" in {
    val diff = TimeUtils.currentTimeDiff(testDate, "days")
    assert(diff == 1)
  }

  "Yesterdays date " should "return number of hours in time diff" in {
    val diff = TimeUtils.currentTimeDiff(testDate, "hours")
    assert(diff >= 23 && diff <= 24)
  }

  "Yesterdays date " should "return number of minutes in time diff" in {
    val diff = TimeUtils.currentTimeDiff(testDate, "minutes")
    assert(diff <= 1441)
  }

  "isStrictlyLessThan" should "return true" in {
    TimeUtils.isStrictlyLessThan("2015-05-19 00:00:00", "2015-06-19 00:00:00") should be (TRUE)
  }

  "isStrictlyLessThan" should "return false" in {
    TimeUtils.isStrictlyLessThan("2015-07-19 00:00:00", "2015-06-19 00:00:00") should be (FALSE)
  }

  "isSameYear" should "return true" in {
    TimeUtils.isSameYear("2015-06-19 00:00:00", "2015-06-20 00:00:00") should be (TRUE)
  }

  "isSameYear" should "return false" in {
    TimeUtils.isSameYear("2014-06-19 00:00:00", "2015-05-19 00:00:00") should be (FALSE)
  }

  "isSameMonth" should "return true" in {
    TimeUtils.isSameMonth("2015-06-19 00:00:00", "2015-06-20 00:00:00") should be (TRUE)
  }

  "isSameMonth" should "return false" in {
    TimeUtils.isSameMonth("2015-06-19 00:00:00", "2015-05-19 00:00:00") should be (FALSE)
  }

  "isSameDay" should "return true" in {
    TimeUtils.isSameDay("2015-06-19 00:00:00", "2015-06-19 00:00:00") should be (TRUE)
  }

  "isSameDay" should "return false" in {
    TimeUtils.isSameDay("2015-06-19 00:00:00", "2015-05-19 00:00:00") should be (FALSE)
  }

  "dayName: String" should "match with expected data" in {
    val text = "20150712"
    val result = TimeUtils.dayName(text, TimeConstants.YYYYMMDD)
    assert(result.toLowerCase.equals("sunday"))
  }

  "nextNDay: String" should "match with expected day" in {
    assert(TimeUtils.nextNDay("Thursday", 0).equals("Thursday"))
    assert(TimeUtils.nextNDay("thursday", 0).equals("Thursday"))
    assert(TimeUtils.nextNDay("Friday", 3).equals("Monday"))
    assert(TimeUtils.nextNDay("wednesday", 6).equals("Tuesday"))
  }

  "getTimeStamp" should "match with expected day" in {
    val sdf = new SimpleDateFormat(TimeConstants.DATE_FORMAT)
    val dt = sdf.parse("2015-06-19")
    val time = new Timestamp(dt.getTime())
    TimeUtils.getTimeStamp("2015-06-19", TimeConstants.DATE_FORMAT) should be (time)
  }

  "getMonthAndYear" should "return Date, month and year" in {
    val dmy = new MonthYear(5, 2015, 19)
    TimeUtils.getMonthAndYear("2015-06-19", TimeConstants.DATE_FORMAT) should be (dmy)
  }

  "getMonthAndYear1" should "return today's date, month and year" in {
    val cal = Calendar.getInstance()
    val dmy = new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
    TimeUtils.getMonthAndYear("", TimeConstants.DATE_FORMAT) should be (dmy)
  }

  "getMonthAndYear2" should "return today's date, month and year" in {
    val cal = Calendar.getInstance()
    val dmy = new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
    TimeUtils.getMonthAndYear(null, TimeConstants.DATE_FORMAT) should be (dmy)
  }

  "getYearFromToday" should "return 0" in {
    TimeUtils.getYearFromToday(null) should be (0)
  }

  "getMaxDaysOfMonth" should "return 30" in {
    TimeUtils.getMaxDaysOfMonth("2015-06-09", TimeConstants.DATE_FORMAT) should be (30)
  }

  "getMaxDaysOfMonth1" should "return last date of the current month" in {
    val cal = Calendar.getInstance()
    TimeUtils.getMaxDaysOfMonth("", TimeConstants.DATE_FORMAT) should be (cal.getActualMaximum(Calendar.DAY_OF_MONTH))
  }

  "getMaxDaysOfMonth2" should "return last date of the current month" in {
    val cal = Calendar.getInstance()
    TimeUtils.getMaxDaysOfMonth(null, TimeConstants.DATE_FORMAT) should be (cal.getActualMaximum(Calendar.DAY_OF_MONTH))
  }

  "getEndTimestampMS" should "return null" in {
    TimeUtils.getEndTimestampMS(null) should be (null)
  }

  "getStartTimestampMS" should "return null" in {
    TimeUtils.getStartTimestampMS(null) should be (null)
  }

  "changeDateFormat" should "return empty string" in {
    val ts: String = null
    TimeUtils.changeDateFormat(ts, TimeConstants.DATE_FORMAT, TimeConstants.DATE_FORMAT_FOLDER) should be ("")
  }

}
