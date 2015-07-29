package com.jabong.dap.data.acq.common

import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class QueryBuilderTest extends FlatSpec with Matchers {
  val condition = "condition"
  val dateCol = Option.apply("dateColumn")
  val jnTbls = Option.apply(List(new JoinTables(name = "testTable1", foreignKey = "fk_testTable1", selectString = null)))
  val lmt1k = Option.apply("1000")

  "getJoinTableStrings" should "return empty strings when joinTables is an empty list" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = null)
    QueryBuilder.getJoinTableStrings(tableInfo) should be ("", "")
  }

  "getJoinTableStrings1" should "return correct strings when tables are passed in joinTables" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = Option.apply(List(
        new JoinTables(name = "testTable1", foreignKey = "fk_testTable1", selectString = null),
        new JoinTables(name = "testTable2", foreignKey = "fk_testTable2", selectString = null)
      )))
    val selectString = ", j1.*, j2.*"
    val joinString = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk LEFT JOIN testTable2 AS j2 ON j2.fk_testTable2 = t1.pk"
    QueryBuilder.getJoinTableStrings(tableInfo) should be (selectString, joinString)
  }

  "getJoinTableStrings2" should "return correct strings when tables are passed in joinTables" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = Option.apply(List(
        new JoinTables(name = "testTable1", foreignKey = "fk_testTable1", selectString = null),
        new JoinTables(name = "testTable2", foreignKey = "fk_testTable2", selectString = Option.apply("j2.fk_testTable2, j2.col1, j2. col2"))
      )))
    val selectString = ", j1.*, j2.fk_testTable2, j2.col1, j2. col2"
    val joinString = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk LEFT JOIN testTable2 AS j2 ON j2.fk_testTable2 = t1.pk"
    QueryBuilder.getJoinTableStrings(tableInfo) should be (selectString, joinString)
  }

  "getFullDataQuery" should "return empty query when driver is not matched" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = jnTbls)
    val driver = "noMatch"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    QueryBuilder.getFullDataQuery(driver, condition, joinSelect, joinFrom, tableInfo) should be ("")
  }

  "getFullDataQuery" should "give correct query for mysql and limit null" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = jnTbls)
    val driver = "mysql"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition  ) AS t"
    QueryBuilder.getFullDataQuery(driver, condition, joinSelect, joinFrom, tableInfo) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit null" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = jnTbls)
    val driver = "sqlserver"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT  t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ) AS t"
    QueryBuilder.getFullDataQuery(driver, condition, joinSelect, joinFrom, tableInfo) should be (query)
  }

  "getFullDataQuery" should "give correct query for mysql and limit not null" in {
    val lmt = Option.apply("limit")
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val driver = "mysql"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ORDER BY pk DESC LIMIT limit) AS t"
    QueryBuilder.getFullDataQuery(driver, condition, joinSelect, joinFrom, tableInfo) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit not null" in {
    val lmt = Option.apply("limit")
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val driver = "sqlserver"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT TOP limit t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ORDER BY pk DESC) AS t"
    QueryBuilder.getFullDataQuery(driver, condition, joinSelect, joinFrom, tableInfo) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit not null and primary key is null" in {
    val lmt = Option.apply("limit")
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = null, mode = "mode",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt, filterCondition = null,
      joinTables = jnTbls)
    val driver = "sqlserver"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT TOP limit t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ) AS t"
    QueryBuilder.getFullDataQuery(driver, condition, joinSelect, joinFrom, tableInfo) should be (query)
  }

  "getDataQuery" should "return correct query for full mode" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "full",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = lmt1k, filterCondition = null,
      joinTables = jnTbls)
    val driver = "mysql"
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ORDER BY pk DESC LIMIT 1000) AS t"
    QueryBuilder.getDataQuery(driver, condition, tableInfo) should be (query)
  }

  "getDataQuery" should "return correct query for daily mode" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "daily",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = jnTbls)
    val driver = null
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition) AS t"
    QueryBuilder.getDataQuery(driver, condition, tableInfo) should be (query)
  }

  "getDataQuery" should "return correct query for hourly mode" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "hourly",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = jnTbls)
    val driver = null
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition) AS t"
    QueryBuilder.getDataQuery(driver, condition, tableInfo) should be (query)
  }

  "getDataQuery" should "return empty query for unknown mode" in {
    val tableInfo = new TableInfo(source = "source", tableName = "tableName", primaryKey = "pk", mode = "abc",
      saveFormat = "parquet", saveMode = "overwrite", dateColumn = dateCol, rangeStart = null, rangeEnd = null,
      limit = null, filterCondition = null,
      joinTables = Option.empty[List[JoinTables]])
    val driver = null
    val query = ""
    QueryBuilder.getDataQuery(driver, condition, tableInfo) should be (query)
  }
}
