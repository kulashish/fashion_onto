package com.jabong.dap.data.acq.common

import java.sql.{ DriverManager, Connection }

import com.jabong.dap.common.json.EmptyClass

/**
 * Created by Rachit on 10/6/15.
 */

// Classes for storing the JSON schema.
case class JoinTables(name: String, foreignKey: String)

case class TableInfo(source: String, tableName: String, primaryKey: String, mode: String, saveFormat: String,
  saveMode: String, dateColumn: String, rangeStart: String, rangeEnd: String, limit: String,
  joinTables: List[JoinTables])

case class ImportInfo(acquisition: List[TableInfo]) extends EmptyClass

object DaoUtil {

  private val driverLoaded = scala.collection.mutable.Map("mysql" -> false, "sqlserver" -> false)

  private def loadDriver(dbc: DbConnection) {
    try {
      if (dbc.driver == "mysql") {
        Class.forName("com.mysql.jdbc.Driver").newInstance
        driverLoaded("mysql") = true
      } else if (dbc.driver == "sqlserver") {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
        driverLoaded("sqlserver") = true
      }

    } catch {
      case e: Exception => {
        println("ERROR: Driver not available: " + e.getMessage)
        throw e
      }
    }
  }

  def getConnection(dbc: DbConnection): Connection = {
    // Only load driver first time
    this.synchronized {
      if (!driverLoaded(dbc.driver)) loadDriver(dbc)
    }

    // Get the connection
    try {
      DriverManager.getConnection(dbc.getConnectionString)
    } catch {
      case e: Exception => {
        println("ERROR: No connection: " + e.getMessage)
        throw e
      }
    }
  }
}
