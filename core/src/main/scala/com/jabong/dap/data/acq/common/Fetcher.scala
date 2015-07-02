package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 8/6/15.
 */
class Fetcher() extends java.io.Serializable {
  def fetch(tableInfo: TableInfo): Unit = {
    val source = tableInfo.source // source
    val tableName = tableInfo.tableName //table name
    val primaryKey = tableInfo.primaryKey // table primary key
    val mode = tableInfo.mode // can be full, daily, monthly
    val dateColumn = tableInfo.dateColumn // column for date
    val saveFormat = tableInfo.saveFormat // orc or parquet
    val saveMode = tableInfo.saveMode // exception, overwrite, etc
    val rangeStart = tableInfo.rangeStart // for range mode (date)
    val rangeEnd = tableInfo.rangeEnd // for range mode (date)
    val limit = tableInfo.limit // for full mode
    val filterCondition = tableInfo.filterCondition
    val joinTables = tableInfo.joinTables //

    val dbConn = new DbConnection(source)

    mode match {
      case "full" => GetData.getFullData(dbConn, source, tableName, primaryKey, limit, filterCondition, saveFormat,
        saveMode, joinTables)
      case "daily" => GetData.getDailyData(dbConn, source, tableName, primaryKey, dateColumn, rangeStart, rangeEnd,
        filterCondition, saveFormat, saveMode, joinTables)
      case "hourly" => GetData.getHourlyData(dbConn, source, tableName, primaryKey, dateColumn, rangeStart, rangeEnd,
        filterCondition, saveFormat, saveMode, joinTables)
    }
  }

}
