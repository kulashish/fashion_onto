package com.jabong.dap.data.acq.common

import java.sql.{ DriverManager, Connection }

import com.jabong.dap.common.json.EmptyClass
import grizzled.slf4j.Logging

/**
 * Case class for storing information about join tables.
 *
 * @param name String The name of the table.
 * @param foreignKey String The name of the foreign key for the table.
 */
case class JoinTables(
  name:       String,
  foreignKey: String
)

/**
 * Case class for storing information about data acquisition from a table.
 *
 * @param source String The source of the data. (Can be erp, bob, unicommerce or nextbee)
 * @param tableName String The name of the table.
 * @param primaryKey String The primary key of the table.
 * @param mode String The mode of the data acquisition. (Can be full, daily or hourly)
 * @param saveFormat String The format in which the data is to be saved. (Can be orc or parquet)
 * @param saveMode String The mode in which the data is to be saved. (Can be overwrite, append, error or ignore)
 * @param dateColumn String The name of the column which represents the date time when the row was updated.
 * @param rangeStart String The date time from which the data is to be fetched.
 * @param rangeEnd String The date time till which the data is to be fetched.
 * @param limit String The number of rows to be fetched.
 * @param filterCondition String Condition to filter the primary key while fetching data.
 * @param joinTables List[JoinTables] List of tables to be joined.
 */
case class TableInfo(
  source:          String,
  tableName:       String,
  primaryKey:      String,
  mode:            String,
  saveFormat:      String,
  saveMode:        String,
  dateColumn:      String,
  rangeStart:      String,
  rangeEnd:        String,
  limit:           String,
  filterCondition: String,
  joinTables:      List[JoinTables]
)

/**
 * Case class for storing the information for the data acquisition.
 *
 * @param acquisition List[TableInfo] List of tables to acquire the data from.
 */
case class ImportInfo(
  acquisition: List[TableInfo]
) extends EmptyClass

/**
 * Object to access ImportInfo variables application wide
 */
object AcqImportInfo {
  var importInfo: ImportInfo = null
  var tableInfo: TableInfo = null
}

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
