package com.jabong.dap.common.time

import java.sql.Timestamp
import java.text.{ DateFormatSymbols, SimpleDateFormat }
import java.util.{ Arrays, Calendar, Date }
import com.jabong.dap.common.StringUtils
import grizzled.slf4j.Logging
import scala.collection.immutable.HashMap

/**
 * Created by Rachit on 28/5/15.
 */
object TimeUtils extends Logging {

  val YESTERDAY_FOLDER = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
  val yesterday = TimeUtils.getDateAfterNDays(-1, _: String)

  /**
   * Returns the total number of days between two given date inputs
   * @param date1
   * @param date2
   * @return
   */
  def daysBetweenTwoDates(date1: Date, date2: Date): Int = {
    (Math.abs(date1.getTime - date2.getTime) / TimeConstants.CONVERT_MILLISECOND_TO_DAYS).toInt
  }

  /**
   * Given a date input, returns the number of days between that date and today's date
   * @param date
   * @return
   */
  def daysFromToday(date: Date): Int = {
    val today = new Date
    daysBetweenTwoDates(today, date).toInt
  }

  def daysFromToday(date: String, dateFormat: String): Int = {
    val format = new SimpleDateFormat(dateFormat)
    val dt = Calendar.getInstance()
    dt.setTime(format.parse(date))
    val today = new Date
    daysBetweenTwoDates(today, dt.getTime)
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
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
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
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
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
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
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
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
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

  def getTimeStamp(): Timestamp = {
    val time = (new Date()).getTime()
    val ts = new Timestamp(time)
    ts
  }

  def getDate(dt: String, dateFormat: String): Date = {
    val sdf = new SimpleDateFormat(dateFormat)
    return sdf.parse(dt)
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
   * get start time of the day
   * @param time
   * @return
   */
  def startOfDay(time: Date): Long = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time.getTime())
    cal.set(Calendar.HOUR_OF_DAY, 0); //set hours to 0
    cal.set(Calendar.MINUTE, 0); // set minutes to 0
    cal.set(Calendar.SECOND, 0); //set seconds to 0
    cal.getTime.getTime
  }

  /**
   * To calculate difference between current time and date provided as argument either in days, minutes hours
   * @param date
   * @param diffType
   * @return
   */
  def currentTimeDiff(date: Timestamp, diffType: String): Double = {
    val cal = Calendar.getInstance()

    val diff = cal.getTime().getTime - date.getTime

    var diffTime: Double = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    diffTime
  }

  /**
   * To calculate difference between start time of previous day and date provided as argument either in days, minutes hours
   * @param date
   * @param diffType
   * @return
   */
  def lastDayTimeDiff(date: Timestamp, diffType: String): Double = {
    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    //val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val diff = startOfDay(cal.getTime) - date.getTime()

    var diffTime: Double = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    diffTime
  }

  /**
   * Input date is string
   * @param date
   * @param diffType
   * @return
   */
  def lastDayTimeDiff(date: String, diffType: String): Double = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val diff = startOfDay(cal.getTime) - prodDate.getTime()

    var diffTime: Double = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    diffTime
  }

  /**
   *
   * @param dt
   * @param dateFormat
   * @return
   */
  def getMonthAndYear(dt: String, dateFormat: String): MonthYear = {
    val cal = Calendar.getInstance()
    if (!StringUtils.isEmpty(dt)) {
      val sdf = new SimpleDateFormat(dateFormat)
      val date = sdf.parse(dt)
      cal.setTime(date)
    }
    new MonthYear(cal.get(Calendar.MONTH), cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_MONTH))
  }

  /**
   *
   * @param dt
   * @param dateFormat
   * @return
   */
  def getMaxDaysOfMonth(dt: String, dateFormat: String): Int = {
    val cal = Calendar.getInstance()
    if (!StringUtils.isEmpty(dt)) {
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

  /**
   *
   * @param dateString
   * @param initialFormat
   * @param expectedFormat
   * @return
   */
  def changeDateFormat(dateString: String, initialFormat: String, expectedFormat: String): String = {
    if (StringUtils.isEmpty(dateString)) {
      return ""
    } else {
      val format = new java.text.SimpleDateFormat(initialFormat)
      format.setLenient(false)
      val date = format.parse(dateString)
      val readableDf = new SimpleDateFormat(expectedFormat);
      //we want to parse date strictly
      return readableDf.format(date)
    }
  }

  /**
   * Overloaded same function with date coming as time stamp
   * @param dateStamp
   * @param initialFormat
   * @param expectedFormat
   * @return
   */
  def changeDateFormat(dateStamp: Timestamp, initialFormat: String, expectedFormat: String): String = {
    if (dateStamp == null) {
      return ""
    } else {
      val format = new java.text.SimpleDateFormat(initialFormat)
      //format.setLenient(false)
      val date = format.parse(dateStamp.toString)
      val readableDf = new SimpleDateFormat(expectedFormat);
      //we want to parse date strictly
      return readableDf.format(date)
    }
  }

  /**
   * @param ipDate
   * @param DateFormat
   * @return Weekday Name
   */
  def dayName(ipDate: String, DateFormat: String): String = {
    //dayformat: yyyyMMdd
    val format = new java.text.SimpleDateFormat(DateFormat)
    format.setLenient(false)
    //we want to parse date strictly
    val date = format.parse(ipDate)

    return (new SimpleDateFormat(TimeConstants.EEEE)).format(date)
  }

  /**
   * @param dayName
   * @param n
   * @return the nth weekday name from dayName
   */
  def nextNDay(dayName: String, n: Int): String = {
    val dfs = new DateFormatSymbols()
    val dayNameCaps = dayName.capitalize
    val weekDays = Arrays.copyOfRange(dfs.getWeekdays(), 1, 8)

    val index = weekDays.indexOf(dayNameCaps)
    if (index == -1) {
      logger.error("I don\'t understand: " + dayName)
    }
    val forwardedIndex = index + n
    return weekDays.apply(forwardedIndex % 7)
  }

  /**
   * This will return Timestamp into YYYY-MM-DD hh:MM:ss.s format
   * @param t1
   * @return
   */
  def getEndTimestampMS(t1: Timestamp): Timestamp = {

    if (t1 == null) {
      return null
    }

    val time = t1.toString()

    return Timestamp.valueOf(time.substring(0, time.indexOf(" ") + 1) + TimeConstants.END_TIME_MS)
  }

  /**
   * This will return Timestamp into YYYY-MM-DD hh:MM:ss.s format
   * @param t1
   * @return
   */
  def getStartTimestampMS(t1: Timestamp): Timestamp = {

    if (t1 == null) {
      return null
    }

    val time = t1.toString()

    return Timestamp.valueOf(time.substring(0, time.indexOf(" ") + 1) + TimeConstants.START_TIME_MS)
  }

  /**
   * Converts integer containing day or month of date to a string with the format MM or dd, respectively.
   */
  def withLeadingZeros(input: Int): String = {
    if (input < 10) {
      "0%s".format(input)
    } else {
      "%s".format(input)
    }
  }

  case class MonthYear(val month: Int, val year: Int, val day: Int)
}
