package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def getFullData(tableName: String, limit: String, source: String,  connectionString: String, saveFormat: String) = {

    val query =  QueryBuilder.getFullDataQuery(source, tableName, limit)
    println(query)

    


  }


}
