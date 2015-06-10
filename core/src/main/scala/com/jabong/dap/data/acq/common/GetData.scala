package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */
object GetData {

  def getFullData(tableName: String, limit: String, source: String,  connectionString: String, saveFormat: String) = {

      val query = if (source == "erp"){
        val limitString = if (limit != null){
          "TOP %s".format(limit)
        }
        else {
          ""
        }
        "(SELECT %s * FROM %s) as t1".format(limitString, tableName)
      }
      else if (source == "bob"){
        val limitString = if (limit != null){
          "LIMIT %s".format(limit)
        }
        else {
          ""
        }
        "(SELECT * FROM %s %s) as t1".format(tableName, limitString)
      }

    println(query)


  }


}
