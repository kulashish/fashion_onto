package com.jabong.dap.data.acq.common

import com.jabong.dap.context.Context

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def cleanString(str: String): String = {
    str.replaceAll("( |-|%)", "")
  }

  def getData(mode: String, driver: String, dbConn: DbConnection, tableName: String, primaryKey: String,
              dateColumn: String, limit: String, rangeStart: String, rangeEnd: String, saveFormat: String,
              saveMode: String): Any = {
    val context = if (saveFormat == "parquet") {
      Context.sqlContext
    } else if (saveFormat == "orc") {
      Context.hiveContext
    } else {
      null
    }

    val condition = QueryBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd)

    val connectionString = dbConn.getConnectionString
    val dbTableQuery = if (mode == "full") {
      QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey)
    } else if (mode == "daily" || mode == "hourly") {
      QueryBuilder.getDataQuery(mode, driver, tableName, rangeStart, rangeEnd, dateColumn, condition)
    } else {
      ""
    }

    println(dbTableQuery)

    val jdbcDF = if (primaryKey == null) {
      context.load("jdbc", Map(
        "url" -> connectionString,
        "dbtable" -> dbTableQuery))
    } else {
      val minMax = GetMinMaxPK.getMinMax(mode, dbConn, tableName, condition, primaryKey, limit)
      println("%s ..... %s".format(minMax.min, minMax.max))
      if (minMax.min == 0 && minMax.max == 0)
        return
      context.load("jdbc", Map(
        "url" -> connectionString,
        "dbtable" -> dbTableQuery,
        "partitionColumn" -> primaryKey,
        "lowerBound" -> minMax.min.toString,
        "upperBound" -> minMax.max.toString,
        "numPartitions" -> "3"))
    }

    jdbcDF.printSchema()
    val columnList = jdbcDF.columns
    val newColumnList = columnList.map(cleanString)
    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)

    newJdbcDF.write.format(saveFormat).mode(saveMode).save("/home/test/sparkData/datadaily")
  }

  def getFullData(driver: String, dbConn: DbConnection, tableName: String, primaryKey: String, limit: String,
                  saveFormat: String, saveMode: String) = {
    getData("full", driver, dbConn, tableName, primaryKey, null, limit, null, null, saveFormat, saveMode)
  }

  def getDailyData(tableName: String, driver: String, dbConn: DbConnection, saveFormat: String, saveMode: String,
                   primaryKey: String, dateColumn: String, rangeStart: String, rangeEnd: String) = {
    getData("daily", driver, dbConn, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, saveFormat, saveMode)
  }

  def getHourlyData(tableName: String, driver: String, dbConn: DbConnection, saveFormat: String, saveMode: String,
                    primaryKey: String, dateColumn: String, rangeStart: String, rangeEnd: String) = {
    getData("hourly", driver, dbConn, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, saveFormat, saveMode)
  }

}
