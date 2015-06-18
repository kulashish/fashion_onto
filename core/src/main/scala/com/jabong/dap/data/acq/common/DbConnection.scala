package com.jabong.dap.data.acq.common

import com.jabong.dap.common.AppConfig

/**
 * Created by Abhay on 9/6/15.
 */
case class DbConnection(source: String) {
  require(source != null, "Source Type is null")
  var driver, server, port, dbName, userName, password = ""

  for (c <- AppConfig.config.credentials) {
    if (c.source == source) {
      driver = c.driver
      server = c.server
      port = c.port
      dbName = c.dbName
      userName = c.userName
      password = c.password
    }
  }

  require(driver != "", "Credentials not provided for source %s".format(source))

  def getConnectionString = {
    driver match {
      case "sqlserver" =>
        "jdbc:sqlserver://%s:%s;database=%s;userName=%s;password=%s".
          format(server, port, dbName, userName, password)

      case "mysql" =>
        "jdbc:mysql://%s:%s/%s?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&user=%s&password=%s"
          .format(server, port, dbName, userName, password)

      case _ => ""
    }
  }

  def getDriver = driver
}
