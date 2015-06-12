package com.jabong.dap.data.acq.common

import java.sql.{ ResultSet, Statement }

/**
 * Created by Abhay on 9/6/15.
 */

case class MinMax(min: Long, max: Long)

object GetMinMaxPK {
  def getMinMax(dbc: DbConnection, tableName: String, cond: String, tablePrimaryKey: String, limit: String): MinMax = {
    var minMax = new MinMax(0, 0)

    val minMaxSql = if (limit == null) {
      if (null != cond && cond.length > 0) {
        "SELECT MIN(%s), MAX(%s) FROM %s WHERE %s".format(tablePrimaryKey, tablePrimaryKey, tableName, cond)
      } else {
        "SELECT MIN(%s), MAX(%s) FROM %s".format(tablePrimaryKey, tablePrimaryKey, tableName)
      }
    } else {
      if (dbc.driver == "mysql") {
        "SELECT MIN(t1.%s), MAX(t1.%s) FROM (SELECT * FROM %s ORDER BY %s desc LIMIT %s ) AS t1".
          format(tablePrimaryKey, tablePrimaryKey, tableName, tablePrimaryKey, limit)
      } else if (dbc.driver == "sqlserver") {
        "SELECT MIN(t1.%s), MAX(t1.%s) FROM (SELECT TOP %s * FROM %s ORDER BY %s desc) AS t1".
          format(tablePrimaryKey, tablePrimaryKey, limit, tableName, tablePrimaryKey)
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
        println("done executing query")

        try {
          while (rs.next()) {
            minMax = new MinMax(rs.getString(1).toLong, rs.getString(2).toLong)
          }
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
