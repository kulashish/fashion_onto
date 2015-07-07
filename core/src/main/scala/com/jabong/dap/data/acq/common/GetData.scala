package com.jabong.dap.data.acq.common

import com.jabong.dap.common.Spark
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

  def getData(dbConn: DbConnection): Any = {
    val primaryKey = AcqImportInfo.tableInfo.primaryKey
    val saveFormat = AcqImportInfo.tableInfo.saveFormat
    val saveMode = AcqImportInfo.tableInfo.saveMode
    val context = getContext(saveFormat)
    val condition = ConditionBuilder.getCondition()

    logger.info(condition)

    val dbTableQuery = QueryBuilder.getDataQuery(dbConn.getDriver, condition)
    logger.info(dbTableQuery)

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
        dbConn.getConnectionProperties)
    }

    jdbcDF.printSchema()
    val columnList = jdbcDF.columns
    val newColumnList = columnList.map(cleanString)
    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)

    val savePath = PathBuilder.getPath()

    newJdbcDF.write.format(saveFormat).mode(saveMode).save(savePath)
  }

}
