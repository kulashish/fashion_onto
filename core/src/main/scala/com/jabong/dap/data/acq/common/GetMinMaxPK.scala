package com.jabong.dap.data.acq.common

import java.sql.{ ResultSet, Statement }

import com.jabong.dap.common.OptionUtils
import grizzled.slf4j.Logging

/**
 * Gets the minimum and maximum value of the Primary Key for effective partitioning for the requested data.
 */

case class MinMax(min: Long, max: Long)

object GetMinMaxPK extends Logging {
  def getMinMax(dbc: DbConnection, condition: String): MinMax = {
    val mode = AcqImportInfo.tableInfo.mode
    val tableName = AcqImportInfo.tableInfo.tableName
    val tablePrimaryKey = AcqImportInfo.tableInfo.primaryKey
    val limit = OptionUtils.getOptValue(AcqImportInfo.tableInfo.limit)

    var minMax = new MinMax(0, 0)

    val minMaxSql = if ((mode == "full" && limit == null) || (mode == "daily") || mode == "hourly" ||  mode == "monthly") {
      "SELECT MIN(%s), MAX(%s) FROM %s AS t1 %s".format(tablePrimaryKey, tablePrimaryKey, tableName, condition)
    } else {
      dbc.driver match {
        case "mysql" => {
          "SELECT MIN(t.%s), MAX(t.%s) FROM (SELECT * FROM %s AS t1 %s ORDER BY %s desc LIMIT %s ) AS t".
            format(tablePrimaryKey, tablePrimaryKey, tableName, condition, tablePrimaryKey, limit)
        }
        case "sqlserver" => {
          "SELECT MIN(t.%s), MAX(t.%s) FROM (SELECT TOP %s * FROM %s AS t1 %s ORDER BY %s desc) AS t".
            format(tablePrimaryKey, tablePrimaryKey, limit, tableName, condition, tablePrimaryKey)
        }
        case _ => ""
      }

    }

    logger.info(minMaxSql)

    val connection = DaoUtil.getConnection(dbc)
    try {
      val stmt: Statement = connection.createStatement
      try {
        logger.info("executing query")
        val rs: ResultSet = stmt.executeQuery(minMaxSql)

        try {
          while (rs.next()) {
            minMax = new MinMax(rs.getString(1).toLong, rs.getString(2).toLong)
          }
        } catch {
          case e: NumberFormatException => logger.error("Data not found in table for requested duration.")
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
