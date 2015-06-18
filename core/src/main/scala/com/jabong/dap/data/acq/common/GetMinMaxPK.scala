package com.jabong.dap.data.acq.common

import java.sql.{ ResultSet, Statement }

/**
 * Created by Abhay on 9/6/15.
 */

case class MinMax(min: Long, max: Long)

object GetMinMaxPK {
  def getMinMax(mode: String, dbc: DbConnection, tableName: String, condition: String, tablePrimaryKey: String, limit: String): MinMax = {
    var minMax = new MinMax(0, 0)

    val minMaxSql = if ((mode == "full" && limit == null) || (mode == "daily") || mode == "hourly") {
      "SELECT MIN(%s), MAX(%s) FROM %s %s".format(tablePrimaryKey, tablePrimaryKey, tableName, condition)
    } else {
      if (dbc.driver == "mysql") {
        "SELECT MIN(t1.%s), MAX(t1.%s) FROM (SELECT * FROM %s %s ORDER BY %s desc LIMIT %s ) AS t1".
          format(tablePrimaryKey, tablePrimaryKey, tableName, condition,  tablePrimaryKey, limit)
      } else if (dbc.driver == "sqlserver") {
        "SELECT MIN(t1.%s), MAX(t1.%s) FROM (SELECT TOP %s * FROM %s %s ORDER BY %s desc) AS t1".
          format(tablePrimaryKey, tablePrimaryKey, limit, tableName, condition, tablePrimaryKey)
      } else {
        ""
      }
    }

    println(minMaxSql)

    val connection = DaoUtil.getConnection(dbc)
    try {
      val stmt: Statement = connection.createStatement
      try {
        println("executing query")
        val rs: ResultSet = stmt.executeQuery(minMaxSql)

        try {
          while (rs.next()) {
            minMax = new MinMax(rs.getString(1).toLong, rs.getString(2).toLong)
          }
        } catch {
          case e: NumberFormatException => println("Data not found in table for requested duration.")
        } finally {
          rs.close()
        }
      } finally {
        stmt.close()
      }
    } finally {
      connection.close()
    }
    minMax
  }
}
