package com.jabong.dap.data.acq.common

import com.jabong.dap.common.Spark
import grizzled.slf4j.Logging

/**
 * Created by Abhay on 10/6/15.
 */
object GetData extends Logging {

  def cleanString(str: String): String = {
    str.replaceAll("( |-|%)", "")
  }

  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

  def getData(mode: String, dbConn: DbConnection, source: String, tableName: String, primaryKey: String,
              dateColumn: String, limit: String, rangeStart: String, rangeEnd: String, filterCondition: String,
              saveFormat: String, saveMode: String): Any = {
    val context = getContext(saveFormat)
    val condition = ConditionBuilder.getCondition(mode, dateColumn, rangeStart, rangeEnd, filterCondition)

    logger.debug(condition)

    val dbTableQuery = mode match {
      case "full" =>
        QueryBuilder.getFullDataQuery(dbConn.getDriver, tableName, limit, primaryKey, condition)
      case "daily" | "hourly" =>
        QueryBuilder.getDataQuery(mode, dbConn.getDriver, tableName, rangeStart, rangeEnd, dateColumn, condition)
      case _ => ""
    }
    logger.debug(dbTableQuery)

    val jdbcDF = if (primaryKey == null) {
      context.load("jdbc", Map(
        "url" -> dbConn.getConnectionString,
        "dbtable" -> dbTableQuery))
    } else {
      val minMax = GetMinMaxPK.getMinMax(mode, dbConn, tableName, condition, primaryKey, limit)
      logger.debug("%s ..... %s".format(minMax.min, minMax.max))
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

  def getFullData(dbConn: DbConnection, source: String, tableName: String, primaryKey: String, limit: String,
                  filterCondition: String, saveFormat: String, saveMode: String) = {
    getData("full", dbConn, source, tableName, primaryKey, null, limit, null, null, filterCondition, saveFormat, saveMode)
  }

  def getDailyData(dbConn: DbConnection, source: String, tableName: String, primaryKey: String, dateColumn: String,
                   rangeStart: String, rangeEnd: String, filterCondition: String, saveFormat: String, saveMode: String) = {
    getData("daily", dbConn, source, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, filterCondition, saveFormat, saveMode)
  }

  def getHourlyData(dbConn: DbConnection, source: String, tableName: String, primaryKey: String, dateColumn: String,
                    rangeStart: String, rangeEnd: String, filterCondition: String, saveFormat: String, saveMode: String) = {
    getData("hourly", dbConn, source, tableName, primaryKey, dateColumn, null, rangeStart, rangeEnd, filterCondition, saveFormat, saveMode)
  }

}
