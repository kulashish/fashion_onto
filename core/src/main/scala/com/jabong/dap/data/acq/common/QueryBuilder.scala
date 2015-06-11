package com.jabong.dap.data.acq.common

import org.apache.derby.impl.sql.compile.TableName

/**
 * Created by test on 10/6/15.
 */
object QueryBuilder {

  def getFullDataQuery(driver: String, tableName: String, limit: String, tablePrimaryKey: String): String = {

    if (driver == "sqlserver") {
      val limitString = if (limit != null) {
        ("TOP %s".format(limit), "ORDER BY %s desc".format(tablePrimaryKey))
      } else {
        ("", "")
      }

      "(SELECT %s * FROM %s %s) as t1".format(limitString._1, tableName, limitString._2)
    } else if (driver == "mysql") {
      val limitString = if (limit != null) {
        ("LIMIT %s".format(limit), "ORDER BY %s desc".format(tablePrimaryKey))
      } else {
        ("", "")
      }
      "(SELECT * FROM %s %s %s) as t1".format(tableName, limitString._1, limitString._2)
    } else {
      "no query"
    }

  }

}
