package com.jabong.dap.data.acq.common

import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class QueryBuilderTest extends FlatSpec with Matchers {
  val condition = "condition"
  val tableName = "tableName"
  val primaryKey = "pk"
  val mode = "mode"


  "getJoinTableStrings" should "return empty strings when joinTables is an empty list" in {
    val joinTablesList = List()
    QueryBuilder.getJoinTableStrings(joinTablesList, primaryKey) should be ("","")
  }

  "getJoinTableStrings" should "return correct strings when tables are passed in joinTables" in {
    val joinTablesList = List(new JoinTables(name = "testTable1", foreignKey =  "fk_testTable1"),
      new JoinTables(name = "testTable2", foreignKey = "fk_testTable2" ))
    val selectString = ", j1.*, j2.*"
    val joinString = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk LEFT JOIN testTable2 AS j2 ON j2.fk_testTable2 = t1.pk"
    QueryBuilder.getJoinTableStrings(joinTablesList, primaryKey) should be (selectString, joinString)
  }


  "getFullDataQuery" should "return empty query when driver is not matched" in {
    val driver = "noMatch"
    val limit = "limit"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition, joinSelect, joinFrom) should be ("")
  }

  "getFullDataQuery" should "give correct query for mysql and limit null" in {
    val driver = "mysql"
    val limit = null
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition  ) AS t"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition, joinSelect, joinFrom) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit null" in {
    val driver = "sqlserver"
    val limit = null
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT  t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ) AS t"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition, joinSelect, joinFrom) should be (query)
  }

  "getFullDataQuery" should "give correct query for mysql and limit not null" in {
    val driver = "mysql"
    val limit = "limit"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ORDER BY pk DESC LIMIT limit) AS t"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition, joinSelect, joinFrom) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit not null" in {
    val driver = "sqlserver"
    val limit = "limit"
    val joinSelect = ", j1.*"
    val joinFrom = " LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk"
    val query = "(SELECT TOP limit t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ORDER BY pk DESC) AS t"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition, joinSelect, joinFrom) should be (query)
  }

  "getDataQuery" should "return correct query for full mode" in {
    val mode = "full"
    val driver = "mysql"
    val limit = "1000"
    val joinTablesList = List(new JoinTables(name = "testTable1", foreignKey =  "fk_testTable1"))
    val primaryKey = "pk"
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition ORDER BY pk DESC LIMIT 1000) AS t"
    QueryBuilder.getDataQuery(mode, driver, tableName, limit, primaryKey, condition, joinTablesList) should be (query)
  }

  "getDataQuery" should "return correct query for daily mode" in {
    val mode = "daily"
    val primaryKey = "pk"
    val joinTablesList = List(new JoinTables(name = "testTable1", foreignKey =  "fk_testTable1"))
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition) AS t"
    QueryBuilder.getDataQuery(mode, null, tableName, null, primaryKey, condition, joinTablesList) should be (query)
  }

  "getDataQuery" should "return correct query for hourly mode" in {
    val mode = "hourly"
    val primaryKey = "pk"
    val joinTablesList = List(new JoinTables(name = "testTable1", foreignKey =  "fk_testTable1"))
    val query = "(SELECT t1.* , j1.* FROM tableName AS t1  LEFT JOIN testTable1 AS j1 ON j1.fk_testTable1 = t1.pk condition) AS t"
    QueryBuilder.getDataQuery(mode, null, tableName, null, primaryKey, condition, joinTablesList) should be (query)
  }

  "getDataQuery" should "return empty query for unknown mode" in {
    val query = ""
    val primaryKey = "pk"
    val joinTablesList = List()
    QueryBuilder.getDataQuery("abc", null, tableName, null, primaryKey, condition, joinTablesList) should be (query)
  }

}
