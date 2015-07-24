package com.jabong.dap.data.acq.common

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.SharedSparkContext
import org.scalatest.FlatSpec

/**
 * Created by Abhay on 22/6/15.
 */
class PathBuilderTest extends FlatSpec with SharedSparkContext {

  val dateCol = Option.apply("dateColumn")
  val jnTbls = Option.apply(List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1", selectString = null)))
  val lmt = Option.apply("100")

  "getPath" should "return empty string if mode is not full, hourly, or daily" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "othermode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    assert(PathBuilder.getPath(AcqImportInfo.tableInfo).equals(""))
  }

  "getPath" should "return correct path if mode is full" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val dateNow = TimeUtils.getTodayDate("yyyy/MM/dd/HH")
    val outputPath = JsonUtils.TEST_RESOURCES + "/input/source/tableName/full/" + dateNow
    assert(PathBuilder.getPath(AcqImportInfo.tableInfo).equals(outputPath))
  }

  "getPath" should "return correct path if mode is daily and both ranges are null" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    val outputPath = JsonUtils.TEST_RESOURCES + "/input/source/tableName/daily/" + dateYesterday
    assert(PathBuilder.getPath(AcqImportInfo.tableInfo).equals(outputPath))
  }

  "getPath" should "return correct path if mode is monthly and both ranges are provided" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "monthly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = Option.apply("2015-06-01 00:00:00"),
      rangeEnd = Option.apply("2015-06-30 23:59:59"), limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val outputPath = JsonUtils.TEST_RESOURCES + "/input/source/tableName/monthly/2015/06/30"
    assert(PathBuilder.getPath(AcqImportInfo.tableInfo).equals(outputPath))
  }

  "getPath" should "return correct path if mode is daily and both ranges are provided" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = Option.apply("2015-06-13 00:00:00"),
      rangeEnd = Option.apply("2015-06-28 23:59:59"), limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val outputPath = JsonUtils.TEST_RESOURCES + "/input/source/tableName/daily/2015/06/28"
    assert(PathBuilder.getPath(AcqImportInfo.tableInfo).equals(outputPath))
  }

  "getPath" should "return correct path if mode is hourly" in {
    AcqImportInfo.tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = Option.apply("2015-06-13 01:00:00"),
      rangeEnd = Option.apply("2015-06-13 15:59:59"), limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val outputPath = JsonUtils.TEST_RESOURCES + "/input/source/tableName/hourly/2015/06/13/15"
    assert(PathBuilder.getPath(AcqImportInfo.tableInfo).equals(outputPath))
  }

}
