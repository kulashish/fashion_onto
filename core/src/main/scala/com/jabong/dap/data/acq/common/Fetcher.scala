package com.jabong.dap.data.acq.common


/**
 * Created by Abhay on 8/6/15.
 */
class Fetcher(tableInfo: TableInfo) extends java.io.Serializable {
  def fetch(): Unit = {

    val source = tableInfo.source                           // source
    val tableName = tableInfo.tableName                    //table name
    val primaryKey = tableInfo.primaryKey                // table primary key
    val mode = tableInfo.mode                           // can be full, daily, monthly
    val dateColumn = tableInfo.dateColumn              // column for date
    val saveFormat = tableInfo.saveFormat               // orc or parquet
    val rangeStart = tableInfo.rangeStart              // for range mode (date)
    val rangeEnd = tableInfo.rangeEnd                 // for range mode (date)
    val limit = tableInfo.limit                        // for full mode
    val joinTables = tableInfo.joinTables              //

    val dbconn = new DbConnection(source)


    val driver="sqlserver"                // pick from source config


    if (mode == "full"){
      GetData.getFullData(tableName, limit, source, dbconn, saveFormat, primaryKey)
    }


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
