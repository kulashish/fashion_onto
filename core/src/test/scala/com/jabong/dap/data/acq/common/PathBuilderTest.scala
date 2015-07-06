package com.jabong.dap.data.acq.common

import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.{ AppConfig, Config }
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Created by Abhay on 22/6/15.
 */
class PathBuilderTest extends FlatSpec with Matchers {
  val config = new Config(basePath = "basePath")
  AppConfig.config = config
  val source = "source"
  val tableName = "tableName"
  var rangeStart: String = null
  var rangeEnd: String = null

  "withLeadingZeros" should "add a zero if input is less than 10" in {
    val input = 7
    val output = "07"
    PathBuilder.withLeadingZeros(input) should be (output)
  }

  "withLeadingZeros" should "not add a zero if input is greater than 9" in {
    val input = 23
    val output = "23"
    PathBuilder.withLeadingZeros(input) should be (output)
  }

  "getPath" should "return empty string if mode is not full, hourly, or daily" in {
    val mode = "otherMode"
    PathBuilder.getPath(mode, source, tableName, rangeStart, rangeEnd) should be ("")
  }

  "getPath" should "return correct path if mode is full" in {
    val mode = "full"
    val dateNow = TimeUtils.getTodayDate("yyyy/MM/dd/HH")
    val outputPath = "basePath/source/tableName/full/" + dateNow + "/"
    PathBuilder.getPath(mode, source, tableName, rangeStart, rangeEnd) should be (outputPath)
  }

  "getPath" should "return correct path if mode is daily and both ranges are null" in {
    val mode = "daily"
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    val outputPath = "basePath/source/tableName/" + dateYesterday + "/"
    PathBuilder.getPath(mode, source, tableName, rangeStart, rangeEnd) should be (outputPath)
  }

  "getPath" should "return correct path if mode is daily and both ranges are provided" in {
    val mode = "daily"
    rangeStart = "2015-06-13 00:00:00"
    rangeEnd = "2015-06-28 23:59:59"
    val outputPath = "basePath/source/tableName/2015/06/13_28"
    PathBuilder.getPath(mode, source, tableName, rangeStart, rangeEnd) should be (outputPath)
  }

  "getPath" should "return correct path if mode is hourly" in {
    val mode = "hourly"
    rangeStart = "2015-06-13 01:00:00"
    rangeEnd = "2015-06-13 15:59:59"
    val outputPath = "basePath/source/tableName/2015/06/13/01_15"
    PathBuilder.getPath(mode, source, tableName, rangeStart, rangeEnd) should be (outputPath)
  }

}
