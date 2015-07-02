package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */

object QueryBuilder {

  def getJoinTableStrings(): (String, String) = {
    val joinTables = AcqImportInfo.tableInfo.joinTables
    val primaryKey = AcqImportInfo.tableInfo.primaryKey
    if (joinTables.isEmpty) {
      return ("", "")
    }

    var selectString = ""
    var joinString = ""
    var count = 1

    for (info <- joinTables) {
      val tableAlias = "j" + count
      selectString = selectString + ", " + tableAlias + ".*"
      joinString = joinString + " LEFT JOIN " + info.name + " AS " + tableAlias + " ON " + tableAlias + "." +
        info.foreignKey + " = t1." + primaryKey
      count = count + 1
    }

    (selectString, joinString)
  }

  def getFullDataQuery(driver: String, condition: String, joinSelect: String, joinFrom: String) = {
    val tableName = AcqImportInfo.tableInfo.tableName
    val limit = AcqImportInfo.tableInfo.limit
    val primaryKey = AcqImportInfo.tableInfo.primaryKey

    driver match {
      case "sqlserver" =>
        val limitString = if (limit != null) {
          ("TOP %s".format(limit), "ORDER BY %s DESC".format(primaryKey))
        } else {
          ("", "")
        }
        "(SELECT %s t1.* %s FROM %s AS t1 %s %s %s) AS t".format(limitString._1, joinSelect, tableName, joinFrom,
          condition, limitString._2)
      case "mysql" =>
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

  def getDataQuery(driver: String, condition: String) = {
    val mode = AcqImportInfo.tableInfo.mode
    val tableName = AcqImportInfo.tableInfo.tableName

    val joinStrings = getJoinTableStrings()
    mode match {
      case "full" => getFullDataQuery(driver, condition, joinStrings._1, joinStrings._2)
      case "daily" | "hourly" => "(SELECT t1.* %s FROM %s %s %s) AS t".format (joinStrings._1, tableName + " AS t1",
        joinStrings._2, condition)
      case _ => ""
    }
  }
}
