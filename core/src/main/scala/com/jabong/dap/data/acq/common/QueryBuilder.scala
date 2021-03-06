package com.jabong.dap.data.acq.common

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.data.storage.DataSets

/**
 * Builds the query which is used to fetch the requested data on the basis of the input parameters passed in the
 * tableJson.
 */

object QueryBuilder {

  def getJoinTableStrings(tableInfo: TableInfo): (String, String) = {
    val joinTables = if (null != tableInfo.joinTables) tableInfo.joinTables.orNull else null
    val primaryKey = tableInfo.primaryKey
    if (null == joinTables || joinTables.isEmpty) {
      return ("", "")
    }

    var selectString = ""
    var joinString = ""
    var count = 1

    for (info <- joinTables) {
      val tableAlias = "j" + count
      val selectStr = OptionUtils.getOptValue(info.selectString)
      if (null != selectStr) {
        selectString = selectString + ", " + selectStr
      } else {
        selectString = selectString + ", " + tableAlias + ".*"
      }
      joinString = joinString + " LEFT JOIN " + info.name + " AS " + tableAlias + " ON " + tableAlias + "." +
        info.foreignKey + " = t1." + primaryKey
      count = count + 1
    }

    (selectString, joinString)
  }

  def getFullDataQuery(driver: String, condition: String, joinSelect: String, joinFrom: String, tableInfo: TableInfo) = {
    val tableName = tableInfo.tableName
    val limit = OptionUtils.getOptValue(tableInfo.limit)
    val primaryKey = tableInfo.primaryKey

    driver match {
      case DataSets.SQLSERVER =>
        val limitString = if (limit != null && primaryKey != null) {
          ("TOP %s".format(limit), "ORDER BY %s DESC".format(primaryKey))
        } else if (limit != null && primaryKey == null) {
          ("TOP %s".format(limit), "")
        } else {
          ("", "")
        }
        "(SELECT %s t1.* %s FROM %s AS t1 %s %s %s) AS t".format(limitString._1, joinSelect, tableName, joinFrom,
          condition, limitString._2)
      case DataSets.MYSQL =>
        val limitString = if (limit != null) {
          ("LIMIT %s".format(limit), "ORDER BY %s DESC".format(primaryKey))
        } else {
          ("", "")
        }
        "(SELECT t1.* %s FROM %s AS t1 %s %s %s %s) AS t".format(joinSelect, tableName, joinFrom, condition,
          limitString._2, limitString._1)
      case _ => ""
    }
  }

  def getDataQuery(driver: String, condition: String, tableInfo: TableInfo) = {
    val mode = tableInfo.mode
    val tableName = tableInfo.tableName

    val joinStrings = getJoinTableStrings(tableInfo)

    mode match {
      case DataSets.FULL => getFullDataQuery(driver, condition, joinStrings._1, joinStrings._2, tableInfo)
      case DataSets.HOURLY_MODE | DataSets.DAILY_MODE | DataSets.MONTHLY_MODE =>
        "(SELECT t1.* %s FROM %s %s %s) AS t".format (joinStrings._1, tableName + " AS t1", joinStrings._2, condition)
      case _ => ""
    }
  }
}
