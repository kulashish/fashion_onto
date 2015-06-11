package com.jabong.dap.data.acq.common

import java.sql.{ResultSet, Statement}


/**
 * Created by Abhay on 9/6/15.
 */
object GetMinMaxPK {
  def getMinMax(dbc: DbConnection, tableName: String, cond: String, tablePrimaryKey: String) : MinMax = {
    var minMax = new MinMax(0,0);
    var minMaxSql = ""
    if (null != cond && cond.length > 0) {
      minMaxSql = "SELECT MIN(%s), MAX(%s) FROM %s WHERE %s".
        format(tablePrimaryKey,tablePrimaryKey,tableName,cond)
    } else {
      minMaxSql = "SELECT MIN(%s), MAX(%s) FROM %s".
        format(tablePrimaryKey, tablePrimaryKey,tableName)
    }
    println(minMaxSql)

    val connection = DaoUtil.getConnection(dbc)
    try {
      val stmt: Statement = connection.createStatement
      try {
        println("executing query")
        val rs: ResultSet  = stmt.executeQuery(minMaxSql)
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
