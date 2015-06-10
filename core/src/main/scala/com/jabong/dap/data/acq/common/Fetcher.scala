package com.jabong.dap.data.acq.common


/**
 * Created by Abhay on 8/6/15.
 */
class Fetcher(tableInfo: TableInfo) extends java.io.Serializable {
  def fetch(): Unit = {

    val source = tableInfo.source
    val tableName = tableInfo.tableName
    val primaryKey = tableInfo.primaryKey
    val mode = tableInfo.mode                  // can be full, incremental, limit , range
    val dateColumn = tableInfo.dateColumn              //
    val saveFormat = tableInfo.saveFormat
    val rangeStart = tableInfo.rangeStart              // for range and limit mode (int and date respectively)
    val rangeEnd = tableInfo.rangeEnd                 // for range and limit mode (int and date respectively)
    val limit = tableInfo.limit
    val joinTables = tableInfo.joinTables

    val dbconn = new DbConnection(source)
    val connectionString = dbconn.getConnectionString
    println(connectionString)

    val condition = ConditionBuilder.getCondition(mode, rangeStart, rangeEnd, dateColumn, primaryKey)
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
