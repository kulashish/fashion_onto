package com.jabong.dap.data.acq.common

import com.jabong.dap.context.Context

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def cleanString(str: String): String = {
    str.replaceAll("( |-|%)", "")
  }

  def getFullData(tableName: String, limit: String, driver: String, dbconn: DbConnection, saveFormat: String, saveMode: String, tablePrimaryKey: String) = {
    val connectionString = dbconn.getConnectionString
    val dbTableQuery = QueryBuilder.getFullDataQuery(driver, tableName, limit, tablePrimaryKey)
    println(dbTableQuery)
    lazy val minMax = GetMinMaxPK.getMinMax(dbconn, tableName, "", tablePrimaryKey, limit)

    println("%s ..... %s".format(minMax.min, minMax.max))

    val jdbcDF = if (tablePrimaryKey == null) {
      Context.hiveContext.load("jdbc", Map(
        "url" -> connectionString,
        "dbtable" -> dbTableQuery))
    } else {
      Context.hiveContext.load("jdbc", Map(
        "url" -> connectionString,
        "dbtable" -> dbTableQuery,
        "partitionColumn" -> tablePrimaryKey,
        "lowerBound" -> minMax.min.toString,
        "upperBound" -> minMax.max.toString,
        "numPartitions" -> "8"))
    }
    jdbcDF.printSchema()
    val columnList = jdbcDF.columns
    val newColumnList = columnList.map(cleanString)
    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)

    newJdbcDF.write.format(saveFormat).mode(saveMode).save("/home/test/Documents")

  }

}
