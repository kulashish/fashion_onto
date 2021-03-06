package com.jabong.dap.data.acq.common

/**
 * Fetches data for each entry in the tableJson acquisition list.
 */
class Fetcher() extends java.io.Serializable {
  def fetch(tableInfo: TableInfo) = {
    val dbConn = new DbConnection(tableInfo.source)
    GetData.getData(dbConn, tableInfo)
  }
}
