package com.jabong.dap.data.acq.common

import com.jabong.dap.context.Context

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def cleanString(str: String): String = {
    str.replaceAll("( |-|%)", "")
  }

  def getFullData(tableName: String, limit: String, driver: String, dbconn: DbConnection, saveFormat: String,
                  saveMode: String, primaryKey: String) = {
    val context = if (driver == "mysql") {
      Context.sqlContext
    } else if (driver == "sqlserver") {
      Context.hiveContext
    } else {
      null
    }
    val connectionString = dbconn.getConnectionString
    val dbTableQuery = QueryBuilder.getFullDataQuery(driver, tableName, limit, primaryKey)
    println(dbTableQuery)
    lazy val minMax = GetMinMaxPK.getMinMax(dbconn, tableName, "", primaryKey, limit)

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

    newJdbcDF.write.format(saveFormat).mode(saveMode).save("/home/test/Documents")
  }

  def getDailyData(tableName: String, driver: String, dbconn: DbConnection, saveFormat: String, saveMode: String,
                   primaryKey: String, dateColumn: String, rangeStart: String, rangeEnd: String) = {
    val condition = QueryBuilder.getCondition(dateColumn, rangeStart, rangeEnd)
    println(condition)
    val connectionString = dbconn.getConnectionString
//    val dbTableQuery = QueryBuilder.getDailyDataQuery(driver, tableName, null, primaryKey)
//    println(dbTableQuery)
//    lazy val minMax = GetMinMaxPK.getMinMax(dbconn, tableName, "", primaryKey, null)
//
//    println("%s ..... %s".format(minMax.min, minMax.max))
//
//    val jdbcDF = if (tablePrimaryKey == null) {
//      Context.hiveContext.load("jdbc", Map(
//        "url" -> connectionString,
//        "dbtable" -> dbTableQuery))
//    } else {
//      Context.hiveContext.load("jdbc", Map(
//        "url" -> connectionString,
//        "dbtable" -> dbTableQuery,
//        "partitionColumn" -> tablePrimaryKey,
//        "lowerBound" -> minMax.min.toString,
//        "upperBound" -> minMax.max.toString,
//        "numPartitions" -> "8"))
//    }
//    jdbcDF.printSchema()
//    val columnList = jdbcDF.columns
//    val newColumnList = columnList.map(cleanString)
//    val newJdbcDF = jdbcDF.toDF(newColumnList: _*)
//
//    newJdbcDF.write.format(saveFormat).mode(saveMode).save("/home/test/Documents")
  }

}
