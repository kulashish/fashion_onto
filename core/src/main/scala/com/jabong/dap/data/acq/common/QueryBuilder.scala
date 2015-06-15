package com.jabong.dap.data.acq.common


import com.jabong.dap.common.utils.Time
/**
 * Created by Abhay on 10/6/15.
 */
object QueryBuilder {

  def getFullDataQuery(driver: String, tableName: String, limit: String, primaryKey: String) = {
    if (driver == "sqlserver") {
      val limitString = if (limit != null) {
        ("TOP %s".format(limit), "ORDER BY %s desc".format(primaryKey))
      } else {
        ("", "")
      }
      ("(SELECT %s * FROM %s %s) as t1".format(limitString._1, tableName, limitString._2), "")
    } else if (driver == "mysql") {
      val limitString = if (limit != null) {
        ("LIMIT %s".format(limit), "ORDER BY %s desc".format(primaryKey))
      } else {
        ("", "")
      }
      ("(SELECT * FROM %s %s %s) as t1".format(tableName, limitString._1, limitString._2),"")
    } else {
      ("","")
    }
  }

  def getDataQuery(mode: String, driver: String, tableName: String, rangeStart: String, rangeEnd: String,
                        dateColumn: String) = {
    val condition = if (rangeStart == null && rangeEnd == null && mode == "daily") {
      val prevDayDate = Time.getYesterdayDate()
      "WHERE %s >= '%s 00:00:00' AND %s <= '%s 23:59:59'".format(dateColumn, prevDayDate, dateColumn, prevDayDate)
    } else {
      "WHERE %s >= '%s' AND %s <= '%s'". format( dateColumn, rangeStart, dateColumn, rangeEnd)
    }
    ("(SELECT * FROM %s %s) AS t1".format(tableName,condition), condition)

  }

  def getCondition(dateColumn: String, rangeStart: String, rangeEnd: String) = {
    if (rangeStart != null && rangeEnd != null && dateColumn != null) {
      "WHERE %s >= '%s 00:00:00' AND %s <= '%s 23:59:59'".format(dateColumn, rangeStart, dateColumn, rangeEnd)
    } else {
      ""
    }
  }
}
