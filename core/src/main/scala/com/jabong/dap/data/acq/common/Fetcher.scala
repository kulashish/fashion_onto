package com.jabong.dap.data.acq.common

import com.jabong.dap.common.AppConfig

/**
 * Created by Abhay on 8/6/15.
 */
class Fetcher(tableInfo: TableInfo) extends java.io.Serializable {
  def fetch(configFilePath: String): Unit = {

    println(AppConfig.config.master)

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
    val joinTables = tableInfo.joinTables //

    val dbConn = new DbConnection(source)

    val driver = if (source == "bob") {
      "mysql" // pick from source config
    } else if (source == "erp") {
      "sqlserver"
    } else {
      ""
    }

    if (mode == "full") {
      GetData.getFullData(driver, source, dbConn, tableName, primaryKey, limit, saveFormat, saveMode)
    } else if (mode == "daily") {
      GetData.getDailyData(tableName,source, driver, dbConn, saveFormat, saveMode, primaryKey, dateColumn, rangeStart, rangeEnd)
    } else if (mode == "hourly") {
      GetData.getHourlyData(tableName,source, driver, dbConn, saveFormat, saveMode, primaryKey, dateColumn, rangeStart, rangeEnd)
    }
  }

}
