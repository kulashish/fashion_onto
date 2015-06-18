package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */

object QueryBuilder {

  def getFullDataQuery(driver: String, tableName: String, limit: String, primaryKey: String, condition: String) = driver match{
    case "sqlserver" =>
      val limitString = if (limit != null) {
        ("TOP %s".format(limit), "ORDER BY %s desc".format(primaryKey))
      } else {
        ("", "")
      }
      "(SELECT %s * FROM %s %s %s) as t1".format(limitString._1, tableName, condition, limitString._2)
    case "mysql" =>
      val limitString = if (limit != null) {
        ("LIMIT %s".format(limit), "ORDER BY %s desc".format(primaryKey))
      } else {
        ("", "")
      }
      "(SELECT * FROM %s %s %s %s) as t1".format(tableName, condition, limitString._1, limitString._2)
    case _ => ""
  }

  def getDataQuery(mode: String, driver: String, tableName: String, rangeStart: String, rangeEnd: String,
                   dateColumn: String, condition: String) = {
    "(SELECT * FROM %s %s) AS t1".format(tableName, condition)

  }
}
