package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def getFullData(tableName: String, limit: String, driver: String,  dbconn: DbConnection, saveFormat: String, tablePrimaryKey: String) = {

    val connectionString = dbconn.getConnectionString

    val query =  QueryBuilder.getFullDataQuery(driver, tableName, limit)
    println(query)


    lazy val minMax = GetMinMaxPK.getMinMax(dbconn, tableName, "", tablePrimaryKey, limit)


    println("%s ...... %s".format(minMax.min,minMax.max))



  }


}
