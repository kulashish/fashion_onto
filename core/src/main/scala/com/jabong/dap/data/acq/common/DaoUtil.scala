package com.jabong.dap.data.acq.common

import java.sql.{ Connection, DriverManager }

import grizzled.slf4j.Logging

/**
 * Created by pooja on 14/7/15.
 */
object DaoUtil extends Logging {

  private val driverLoaded = scala.collection.mutable.Map("mysql" -> false, "sqlserver" -> false)

  private def loadDriver(dbc: DbConnection) {
    try {
      dbc.driver match {
        case "mysql" =>
          Class.forName ("com.mysql.jdbc.Driver").newInstance
          driverLoaded ("mysql") = true
        case "sqlserver" =>
          Class.forName ("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance
          driverLoaded ("sqlserver") = true
      }
    } catch {
      case e: Exception => {
        logger.error("Driver not available: " + e.getMessage)
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
      DriverManager.getConnection(dbc.getConnectionString, dbc.getConnectionProperties)
    } catch {
      case e: Exception => {
        logger.error("No connection: " + e.getMessage)
        throw e
      }
    }
  }
}
