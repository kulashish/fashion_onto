package com.jabong.dap.data.acq.common

import com.jabong.dap.common.utils.Time
import org.scalatest.{Matchers, FlatSpec}

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
    val filterCondition = "filterCondition"
    val mode = "full"
    val output = "WHERE filterCondition"
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
    val output = "WHERE dateColumn >= '%s 00:00:00' AND dateColumn <= '%s 23:59:59' ". format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd are null and filterCondition is not null" in {
    val mode = "daily"
    val filterCondition = "filterCondition"
    val prevDayDate = Time.getYesterdayDate()
    val output = "WHERE dateColumn >= '%s 00:00:00' AND dateColumn <= '%s 23:59:59' AND filterCondition". format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode hourly and filterCondition is null" in {
    val mode = "hourly"
    rangeStart = "rangeStart"
    rangeEnd =  "rangeEnd"
    val filterCondition = null
    val output = "WHERE dateColumn >= 'rangeStart' AND dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }

  "getCondition" should "return correct condition when mode hourly and filterCondition is not null" in {
    val mode = "hourly"
    val filterCondition = "filterCondition"
    rangeStart = "rangeStart"
    rangeEnd =  "rangeEnd"
    val output = "WHERE dateColumn >= 'rangeStart' AND dateColumn <= 'rangeEnd' AND filterCondition"
    ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition) should be (output)
  }


}
