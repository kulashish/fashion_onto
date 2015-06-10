package com.jabong.dap.data.acq.common

import org.apache.derby.impl.sql.compile.TableName

/**
 * Created by test on 10/6/15.
 */
object QueryBuilder {

  def getFullDataQuery (driver: String, tableName: String, limit: String): String = {

    if (driver == "sqlserver"){
      val limitString = if (limit != null){
        "TOP %s".format(limit)
      }
      else {
        ""
      }
      "(SELECT %s * FROM %s) as t1".format(limitString, tableName)
    }
    else if (driver == "mysqlserver"){
      val limitString = if (limit != null){
        "LIMIT %s".format(limit)
      }
      else {
        ""
      }
      "(SELECT * FROM %s %s) as t1".format(tableName, limitString)
    }
    else {
      ""
    }

  }


}
