package com.jabong.dap.data.acq.common

import com.jabong.dap.common.utils.Time
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class ConditionBuilderTest extends FlatSpec with Matchers {
  val dateColumn = "dateColumn"
  var rangeStart: String = null
  var rangeEnd: String = null

  "getCondition" should "return empty string when mode is not full, daily or hourly" in {
    val filterCondition = null
    val mode = "otherMode"
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be ("")
  }

  "getCondition" should "return correct condition when filterCondition is not null and mode is full" in {
    val filterCondition = "tableColumn NOT LIKE 'R%'"
    val mode = "full"
    val output = "WHERE tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when filterCondition is null and mode is full" in {
    val mode = "full"
    val filterCondition = null
    val output = ""
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd, and filterCondition are null" in {
    val mode = "daily"
    val filterCondition = null
    val prevDayDate = Time.getYesterdayDate()
    val output = "WHERE t1.dateColumn >= '%s 00:00:00' AND t1.dateColumn <= '%s 23:59:59' ".format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd are null and filterCondition is not null" in {
    val mode = "daily"
    val filterCondition = "tableColumn NOT LIKE 'R..'"
    val prevDayDate = Time.getYesterdayDate()
    val output = "WHERE t1.dateColumn >= '%s 00:00:00' AND t1.dateColumn <= '%s 23:59:59' AND tableColumn NOT LIKE 'R..'".format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is hourly and filterCondition is null" in {
    val mode = "hourly"
    rangeStart = "rangeStart"
    rangeEnd = "rangeEnd"
    val filterCondition = null
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is hourly and filterCondition is not null" in {
    val mode = "hourly"
    val filterCondition = "tableColumn NOT LIKE 'R%'"
    rangeStart = "rangeStart"
    rangeEnd = "rangeEnd"
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' AND tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily with ranges not null and filterCondition null" in {
    val mode = "daily"
    rangeStart = "rangeStart"
    rangeEnd = "rangeEnd"
    val filterCondition = null
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily with ranges not null and filterCondition is not null" in {
    val mode = "daily"
    val filterCondition = "tableColumn NOT LIKE 'R%'"
    rangeStart = "rangeStart"
    rangeEnd = "rangeEnd"
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' AND tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

}
