package com.jabong.dap.data.acq.common

import com.jabong.dap.common.utils.Time
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class ConditionBuilderTest extends FlatSpec with Matchers {
  "getCondition" should "return empty string when mode is not full, daily or hourly" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "othermode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    ConditionBuilder.getCondition() should be ("")
  }

  "getCondition" should "return correct condition when filterCondition is not null and mode is full" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = "tableColumn NOT LIKE 'R%'",
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val output = "WHERE tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when filterCondition is null and mode is full" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val output = ""
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd, and filterCondition are null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val prevDayDate = Time.getYesterdayDate()
    val output = "WHERE t1.dateColumn >= '%s 00:00:00' AND t1.dateColumn <= '%s 23:59:59' ".format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when mode is daily and rangeStart, rangeEnd are null and filterCondition is not null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = null, rangeEnd = null,
      limit = "100", filterCondition = "tableColumn NOT LIKE 'R..'",
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val prevDayDate = Time.getYesterdayDate()
    val output = "WHERE t1.dateColumn >= '%s 00:00:00' AND t1.dateColumn <= '%s 23:59:59' AND tableColumn NOT LIKE 'R..'".format(prevDayDate, prevDayDate)
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when mode is hourly and filterCondition is null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = "rangeStart",
      rangeEnd = "rangeEnd", limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when mode is hourly and filterCondition is not null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = "rangeStart",
      rangeEnd = "rangeEnd", limit = "100", filterCondition = "tableColumn NOT LIKE 'R%'",
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' AND tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when mode is daily with ranges not null and filterCondition null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = "rangeStart",
      rangeEnd = "rangeEnd", limit = "100", filterCondition = null,
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' "
    ConditionBuilder.getCondition() should be (output)
  }

  "getCondition" should "return correct condition when mode is daily with ranges not null and filterCondition is not null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = null, tableName = null, primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = "dateColumn", rangeStart = "rangeStart",
      rangeEnd = "rangeEnd", limit = "100", filterCondition = "tableColumn NOT LIKE 'R%'",
      joinTables = List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1")))
    val output = "WHERE t1.dateColumn >= 'rangeStart' AND t1.dateColumn <= 'rangeEnd' AND tableColumn NOT LIKE 'R%'"
    ConditionBuilder.getCondition() should be (output)
  }

}
