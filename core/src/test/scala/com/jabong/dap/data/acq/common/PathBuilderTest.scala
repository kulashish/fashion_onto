package com.jabong.dap.data.acq.common

import java.io.File

import com.jabong.dap.common.{ AppConfig, Config }
import com.jabong.dap.common.time.TimeUtils
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class PathBuilderTest extends FlatSpec with Matchers {
  val config = new Config(basePath = "basePath")
  AppConfig.config = config

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
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "othermode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    PathBuilder.getPath() should be ("")
  }

  "getPath" should "return correct path if mode is full" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val dateNow = TimeUtils.getTodayDateWithHrs().replaceAll("-", File.separator)
    val outputPath = "basePath/source/tableName/full/" + dateNow + "/"
    PathBuilder.getPath() should be (outputPath)
  }

  "getPath" should "return correct path if mode is daily and both ranges are null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val dateYesterday = TimeUtils.getYesterdayDate().replaceAll("-", File.separator)
    val outputPath = "basePath/source/tableName/" + dateYesterday + "/"
    PathBuilder.getPath() should be (outputPath)
  }

  "getPath" should "return correct path if mode is daily and both ranges are provided" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = "2015-06-13 00:00:00",
      rangeEnd = "2015-06-28 23:59:59", limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val outputPath = "basePath/source/tableName/2015/06/13_28"
    PathBuilder.getPath() should be (outputPath)
  }

  "getPath" should "return correct path if mode is hourly" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = "2015-06-13 01:00:00",
      rangeEnd = "2015-06-13 15:59:59", limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val outputPath = "basePath/source/tableName/2015/06/13/01_15"
    PathBuilder.getPath() should be (outputPath)
  }

}
