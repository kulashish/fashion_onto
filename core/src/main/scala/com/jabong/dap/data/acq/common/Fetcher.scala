package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 8/6/15.
 */
class Fetcher() extends java.io.Serializable {
  def fetch(tableInfo: TableInfo): Unit = {
    val dbConn = new DbConnection(AcqImportInfo.tableInfo.source)
    GetData.getData(dbConn)
  }
}
