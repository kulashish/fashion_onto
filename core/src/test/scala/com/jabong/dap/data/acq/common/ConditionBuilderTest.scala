package com.jabong.dap.data.acq.common

import com.jabong.dap.common.time.{ Constants, TimeUtils }
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class ConditionBuilderTest extends FlatSpec with Matchers {
  val dateCol = Option.apply("dateColumn")
  val lmt = Option.apply("100")
  val jnTbls = Option.apply(List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
  val fltrCond = Option.apply("tableColumn NOT LIKE 'R%'")
  val rngStrt = Option.apply("rangeStart")
  val rngEnd = Option.apply("rangeEnd")

  "getCondition" should "return empty string when mode is not full, daily or hourly" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "othermode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    ConditionBuilder.getCondition(tableInfo) should be ("")
  }

  "getCondition" should "return correct condition when filterCondition is not null and mode is full" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = fltrCond,
      joinTables = jnTbls)
    val output = "WHERE tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when filterCondition is null and mode is full" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val output = ""
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd, and filterCondition are null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val prevDayDate = TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT)
    val output = "WHERE t1.dateColumn >= '%s 00:00:00' AND t1.dateColumn <= '%s 23:59:59' ".format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd are null and filterCondition is not null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = Option.apply("tableColumn NOT LIKE 'R..'"),
      joinTables = jnTbls)
    val prevDayDate = TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT)
    val output = "WHERE t1.dateColumn >= '%s 00:00:00' AND t1.dateColumn <= '%s 23:59:59' AND tableColumn NOT LIKE 'R..'".format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is hourly and filterCondition is null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = rngStrt,
      rangeEnd = rngEnd, limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is hourly and filterCondition is not null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = rngStrt,
      rangeEnd = rngEnd, limit = lmt, filterCondition = fltrCond,
      joinTables = jnTbls)
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' AND tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily with ranges not null and filterCondition null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = rngStrt,
      rangeEnd = rngEnd, limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is daily with ranges not null and filterCondition is not null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = rngStrt,
      rangeEnd = rngEnd, limit = lmt, filterCondition = fltrCond,
      joinTables = jnTbls)
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' AND tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

  "getCondition" should "return correct condition when mode is monthly with ranges not null and filterCondition null" in {
    val tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "monthly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = rngStrt,
      rangeEnd = rngEnd, limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition(tableInfo) should be (output)
  }

}
