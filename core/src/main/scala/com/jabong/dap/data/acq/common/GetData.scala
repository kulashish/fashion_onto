package com.jabong.dap.data.acq.common

import com.jabong.dap.common.{ StringUtils, Spark }
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Gets the data for a given DbConnection
 */
object GetData extends Logging {

  def getData(dbConn: DbConnection, tableInfo: TableInfo): Any = {
    val savePath = PathBuilder.getPath(tableInfo)
    val saveMode = tableInfo.saveMode

    if (!DataWriter.canWrite(saveMode, savePath)) {
      return
    }

    val condition = ConditionBuilder.getCondition(dbConn.getDriver, tableInfo)
    logger.info(condition)

    val dbTableQuery = QueryBuilder.getDataQuery(dbConn.getDriver, condition, tableInfo)
    logger.info(dbTableQuery)

    val primaryKey = tableInfo.primaryKey
    val saveFormat = tableInfo.saveFormat
    val context = Spark.getContext(saveFormat)

    val jdbcDF: DataFrame = if (primaryKey == null) {
      context.read.jdbc(dbConn.getConnectionString, dbTableQuery, dbConn.getConnectionProperties)
    } else {
      val minMax = GetMinMaxPK.getMinMax(dbConn, condition)
      logger.info("%s ..... %s".format(minMax.min, minMax.max))
      if (minMax.min == 0 && minMax.max == 0) {
        println("Data for the given date and table is null: " + dbTableQuery)
        context.read.jdbc(
          dbConn.getConnectionString,
          dbTableQuery,
          dbConn.getConnectionProperties
        )
      } else {
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
    }

    //    jdbcDF.printSchema()
    val columnList = jdbcDF.columns
    val newColumnList = columnList.map(StringUtils.cleanString)
    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)

    newJdbcDF.write.format(saveFormat).mode(saveMode).save(savePath)
    println("Data written successfully using query: " + dbTableQuery)
    context.clearCache()
  }
}

