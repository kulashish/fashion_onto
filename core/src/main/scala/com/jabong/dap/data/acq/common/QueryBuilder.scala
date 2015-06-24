package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */

object QueryBuilder {

  def getFullDataQuery(driver: String, tableName: String, limit: String, primaryKey: String, condition: String) =
    driver match {
      case "sqlserver" =>
        val limitString = if (limit != null) {
          ("TOP %s".format(limit), "ORDER BY %s DESC".format(primaryKey))
        } else {
          ("", "")
        }
        "(SELECT %s * FROM %s %s %s) AS t1".format(limitString._1, tableName, condition, limitString._2)
      case "mysql" =>
        val limitString = if (limit != null) {
          ("LIMIT %s".format(limit), "ORDER BY %s DESC".format(primaryKey))
        } else {
          ("", "")
        }
        "(SELECT * FROM %s %s %s %s) AS t1".format(tableName, condition, limitString._2, limitString._1)
      case _ => ""
    }

  def getDataQuery(mode: String, driver: String, tableName: String, limit: String, primaryKey: String,
                   condition: String) = {
    mode match {
      case "full" => getFullDataQuery(driver, tableName, limit, primaryKey, condition)
      case "daily" | "hourly" => "(SELECT * FROM %s %s) AS t1".format (tableName, condition)
      case _ => ""
    }
  }
}
