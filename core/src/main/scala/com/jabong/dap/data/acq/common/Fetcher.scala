package com.jabong.dap.data.acq.common

import com.jabong.dap.context.Context


/**
 * Created by Abhay on 8/6/15.
 */
class Fetcher(master: String) extends java.io.Serializable {
  def fetch(): Unit = {

    val source = "erp"
    val tableName = "tabletest"
    val primaryKey = "pktest"
    val mode = "full"                  // can be full, incremental, limit , range
    val dateColumn = null               //
    val rangeFrom = null               // for range and limit mode (int and date respectively)
    val rangeTo = null                 // for range and limit mode (int and date respectively)
    val limit = null
    val joinTables = null

    val dbconn = new DbConnection("sqlserver","1.1.1.1","1234","Jasda","user","password")
    val connectionString = dbconn.getConnectionString
    println(connectionString)

    val condition = ConditionBuilder.getCondition(mode, rangeFrom, rangeTo, dateColumn, primaryKey)
    println("condition: %s".format(condition))

    val dbTableQuery = "(SELECT * FROM %s %s) AS t1".format(tableName, condition)
    println("dbtablequery: %s".format(dbTableQuery))

//    val jdbcDF = {
//      if (primaryKey != null) {
//        val minMax = GetMinMaxPK.getMinMax(dbconn, tableName, condition, primaryKey)
//        Context.hiveContext.load(
//          "jdbc",
//          Map(
//            "url" -> connectionString,
//            "dbtable" -> dbTableQuery,
//            "partitionColumn" -> primaryKey,
//            "lowerBound" -> minMax.min.toString,
//            "upperBound" -> minMax.max.toString,
//            "numPartitions" -> "4"
//          )
//        )
//      }
//      else{
//        Context.hiveContext.load(
//          "jdbc",
//          Map(
//          "url" -> connectionString,
//          "dbtable" -> dbTableQuery
//          )
//        )
//      }
//
//    }
  }










}
