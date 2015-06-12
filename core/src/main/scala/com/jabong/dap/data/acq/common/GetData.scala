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
    saveMode: String) = {
    val context = if (saveFormat == "parquet") {
      Context.sqlContext
    } else if (saveFormat == "orc") {
      Context.hiveContext
    } else {
      null
    }

    val connectionString = dbConn.getConnectionString
    val dbTableQuery = QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey)
    println(dbTableQuery)
    lazy val minMax = GetMinMaxPK.getMinMax(dbConn, tableName, "", primaryKey, limit)
    println("%s ..... %s".format(minMax.min, minMax.max))

    val jdbcDF = if (primaryKey == null) {
      context.load("jdbc", Map(
        "url" -> connectionString,
        "dbtable" -> dbTableQuery))
    } else {
      context.load("jdbc", Map(
        "url" -> connectionString,
        "dbtable" -> dbTableQuery,
        "partitionColumn" -> primaryKey,
        "lowerBound" -> minMax.min.toString,
        "upperBound" -> minMax.max.toString,
        "numPartitions" -> "8"))
    }

    jdbcDF.printSchema()
    val columnList = jdbcDF.columns
    val newColumnList = columnList.map(cleanString)
    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)

    newJdbcDF.write.format(saveFormat).mode(saveMode).save("/home/test/sparkData/data")
  }

  def getFullData(driver: String, dbConn: DbConnection, tableName: String, primaryKey: String, limit: String,
    saveFormat: String, saveMode: String) = {
    getData("full", driver, dbConn, tableName, primaryKey, null, limit, null, null, saveFormat, saveMode)
  }

  def getDailyData(tableName: String, driver: String, dbConn: DbConnection, saveFormat: String, saveMode: String,
    primaryKey: String, dateColumn: String, rangeStart: String, rangeEnd: String) = {
    getData("daily", driver, dbConn, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, saveFormat, saveMode)
  }

}
