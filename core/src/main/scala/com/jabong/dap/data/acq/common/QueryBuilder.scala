package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 10/6/15.
 */

object QueryBuilder {

  def getJoinTableStrings(joinTables: List[JoinTables], primaryKey: String):
    (String, String) = {
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

  def getFullDataQuery(driver: String, tableName: String, limit: String, primaryKey: String, condition: String,
                        joinSelect: String, joinFrom: String) =
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

  def getDataQuery(mode: String, driver: String, tableName: String, limit: String, primaryKey: String,
                   condition: String, joinTables: List[JoinTables]) = {
    val joinStrings = getJoinTableStrings(joinTables, primaryKey)
    mode match {
      case "full" => getFullDataQuery(driver, tableName, limit, primaryKey, condition, joinStrings._1, joinStrings._2)
      case "daily" | "hourly" => "(SELECT t1.* %s FROM %s %s %s) AS t".format (joinStrings._1, tableName + " AS t1",
        joinStrings._2, condition)
      case _ => ""
    }
  }
}
