package com.jabong.dap.data.acq.common

import java.util.Properties

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

  def getConnectionProperties = {
    val connProp = new Properties()
    driver match {
      case "sqlserver" =>
        connProp.put("userName", userName)
        connProp.put("password", password)
        connProp
      case "mysql" =>
        connProp.put("user", userName)
        connProp.put("password", password)
        connProp.put("zeroDateTimeBehavior", "convertToNull")
        connProp.put("tinyInt1isBit", "false")
        connProp
      case _ => null
    }
  }

  def getConnectionString = {
    driver match {
      case "sqlserver" =>
        "jdbc:sqlserver://%s:%s;database=%s".
          format(server, port, dbName)
      //        "jdbc:sqlserver://%s:%s;database=%s;userName=%s;password=%s".
      //          format(server, port, dbName, userName, password)
      case "mysql" =>
        "jdbc:mysql://%s:%s/%s"
          .format(server, port, dbName)
      //        "jdbc:mysql://%s:%s/%s?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&user=%s&password=%s"
      //          .format(server, port, dbName, userName, password)
      case _ => ""
    }
  }

  def getDriver = driver
}
