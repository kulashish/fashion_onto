package com.jabong.dap.data.acq.common

import com.jabong.dap.common.Spark

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def cleanString(str: String): String = {
    str.replaceAll("( |-|%)", "")
  }

  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

  def getData(mode: String, source: String, driver: String, dbConn: DbConnection, tableName: String, primaryKey: String,
              dateColumn: String, limit: String, rangeStart: String, rangeEnd: String, saveFormat: String,
              saveMode: String , filterCondition: String): Any = {
    val context = getContext(saveFormat)
    val condition = ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition)

    println(condition)

    val dbTableQuery = if (mode == "full") {
      QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey, condition)
    } else if (mode == "daily" || mode == "hourly") {
      QueryBuilder.getDataQuery(mode, driver, tableName, rangeStart, rangeEnd, dateColumn, condition)
    } else {
      ""
    }

    println(dbTableQuery)

    val jdbcDF = if (primaryKey == null) {
      context.load("jdbc", Map(
        "url" -> dbConn.getConnectionString,
        "dbtable" -> dbTableQuery))
    } else {
      val minMax = GetMinMaxPK.getMinMax(mode, dbConn, tableName, condition, primaryKey, limit)
      println("%s ..... %s".format(minMax.min, minMax.max))
      if (minMax.min == 0 && minMax.max == 0)
        return
      context.load("jdbc", Map(
        "url" -> dbConn.getConnectionString,
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

    val savePath = PathBuilder.getPath(mode, source, tableName, rangeStart, rangeEnd)

    newJdbcDF.write.format(saveFormat).mode(saveMode).save(savePath)
  }

  def getFullData(driver: String, source: String, dbConn: DbConnection, tableName: String, primaryKey: String, limit: String,
                  filterCondition: String, saveFormat: String, saveMode: String) = {
    getData("full", source, driver, dbConn, tableName, primaryKey, null, limit, null, null, saveFormat, saveMode, filterCondition)
  }

  def getDailyData(tableName: String, source: String, driver: String, dbConn: DbConnection, saveFormat: String, saveMode: String,
                   primaryKey: String, dateColumn: String, rangeStart: String, rangeEnd: String, filterCondition: String) = {
    getData("daily", source, driver, dbConn, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, saveFormat, saveMode,  filterCondition)
  }

  def getHourlyData(tableName: String, source: String, driver: String, dbConn: DbConnection, saveFormat: String, saveMode: String,
                    primaryKey: String, dateColumn: String, rangeStart: String, rangeEnd: String, filterCondition: String) = {
    getData("hourly", source, driver, dbConn, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, saveFormat, saveMode, filterCondition)
  }

}
