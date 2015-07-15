package com.jabong.dap.data.acq.common

import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.merge.common.DataVerifier
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Gets the data for a given DbConnection
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

  def getData(dbConn: DbConnection, tableInfo: TableInfo): Any = {
    val savePath = PathBuilder.getPath(tableInfo)
    val saveMode = tableInfo.saveMode

    if (saveMode.equals("ignore")) {
      if (DataVerifier.hdfsDataExists(savePath)) {
        logger.info("File Already exists: " + savePath)
        println("File Already exists so not doing anything: " + savePath)
        return
      }
      if (DataVerifier.hdfsDirExists(savePath)) {
        DataVerifier.hdfsDirDelete(savePath)
        logger.info("Directory with no success file was removed: " + savePath)
        println("Directory with no success file was removed: " + savePath)
      }
    } else if (saveMode.equals("error") && DataVerifier.hdfsDirExists(savePath)) {
      logger.info("File Already exists and save Mode is error: " + savePath)
      println("File Already exists and save Mode is error: " + savePath)
      return
    }

    val condition = ConditionBuilder.getCondition(tableInfo)
    logger.info(condition)

    val dbTableQuery = QueryBuilder.getDataQuery(dbConn.getDriver, condition, tableInfo)
    logger.info(dbTableQuery)

    val primaryKey = tableInfo.primaryKey
    val saveFormat = tableInfo.saveFormat
    val context = getContext(saveFormat)

    val jdbcDF: DataFrame = if (primaryKey == null) {
      context.read.jdbc(dbConn.getConnectionString, dbTableQuery, dbConn.getConnectionProperties)
    } else {
      val minMax = GetMinMaxPK.getMinMax(dbConn, condition)
      logger.info("%s ..... %s".format(minMax.min, minMax.max))
      if (minMax.min == 0 && minMax.max == 0)
        return null
      context.read.jdbc(
        dbConn.getConnectionString,
        dbTableQuery,
        primaryKey,
        minMax.min,
        minMax.max,
        3,
        dbConn.getConnectionProperties
      )
    }

    jdbcDF.printSchema()
    val columnList = jdbcDF.columns
    val newColumnList = columnList.map(cleanString)
    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)

    newJdbcDF.write.format(saveFormat).mode(saveMode).save(savePath)
  }
}

