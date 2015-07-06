package com.jabong.dap.data.acq.common

/**
 * Fetches data for each entry in the tableJson acquisition list.
 */
class Fetcher() extends java.io.Serializable {
  def fetch(tableInfo: TableInfo): Unit = {
    val dbConn = new DbConnection(AcqImportInfo.tableInfo.source)
    GetData.getData(dbConn)
  }
}
