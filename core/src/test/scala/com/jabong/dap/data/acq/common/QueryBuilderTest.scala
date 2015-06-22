package com.jabong.dap.data.acq.common

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by Abhay on 22/6/15.
 */
class QueryBuilderTest extends FlatSpec with Matchers{
  val condition = "condition"
  val tableName = "tableName"
  val primaryKey = "pk"
  val mode = "mode"


  "getFullDataQuery" should "return empty query when driver is not matched" in {
    val driver = "noMatch"
    val limit = "limit"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition ) should be ("")
  }

  "getFullDataQuery" should "give correct query for mysql and limit null" in {
    val driver = "mysql"
    val limit = null
    val query = "(SELECT * FROM tableName condition  ) as t1"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition ) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit null" in {
    val driver = "sqlserver"
    val limit = null
    val query = "(SELECT  * FROM tableName condition ) as t1"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition ) should be (query)
  }

  "getFullDataQuery" should "give correct query for mysql and limit not null" in {
    val driver = "mysql"
    val limit = "limit"
    val query = "(SELECT * FROM tableName condition ORDER BY pk desc LIMIT limit) as t1"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition ) should be (query)
  }

  "getFullDataQuery" should "give correct query for sqlserver and limit not null" in {
    val driver = "sqlserver"
    val limit = "limit"
    val query = "(SELECT TOP limit * FROM tableName condition ORDER BY pk desc) as t1"
    QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition ) should be (query)
  }

  "getDataQuery" should "return correct query" in {
    val driver = "anyDriver"
    val rangeStart = "rangeStart"
    val rangeEnd = "rangeEnd"
    val dateColumn = "dateColumn"
    val query = "(SELECT * FROM tableName condition) AS t1"
    QueryBuilder.getDataQuery(mode, driver, tableName, rangeStart, rangeEnd, dateColumn, condition) should be (query)
  }

}
