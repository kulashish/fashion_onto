package com.jabong.dap.data.acq.common

import com.jabong.dap.common.{ AppConfig, Config, Credentials }
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Abhay on 22/6/15.
 */
class DbConnectionTest extends FlatSpec with Matchers {

  "dbConnection" should "throw an exception for null source" in {
    a[IllegalArgumentException] should be thrownBy {
      new DbConnection(null)
    }
  }

  "dbConnection" should "throw an exception if credentials not found for source" in {
    val credentials = new Credentials(source = "erp", driver = null, server = null, port = null, dbName = null,
      userName = null, password = null)
    val credentialsList = List(credentials)
    val config = new Config(applicationName = null, master = null, basePath = null, credentials = credentialsList)
    AppConfig.config = config
    a[IllegalArgumentException] should be thrownBy {
      new DbConnection("bob")
    }
  }

  "getConnectionString" should "return correct connection string for mysql driver" in {
    val credentials = new Credentials(source = "source", driver = "mysql", server = "1.2.3.4", port = "1234",
      dbName = "dbTest", userName = "mark", password = "antony")
    val credentialsList = List(credentials)
    val config = new Config(applicationName = null, master = null, basePath = null, credentials = credentialsList)
    AppConfig.config = config
    val dbConnection = new DbConnection("source")
    val output = "jdbc:mysql://1.2.3.4:1234/dbTest?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&user=mark&password=antony"
    dbConnection.getConnectionString should be (output)
  }

  "getConnectionString" should "return correct connection string for sqlserver driver" in {
    val credentials = new Credentials(source = "source", driver = "sqlserver", server = "1.2.3.4", port = "1234",
      dbName = "dbTest", userName = "mark", password = "antony")
    val credentialsList = List(credentials)
    val config = new Config(applicationName = null, master = null, basePath = null, credentials = credentialsList)
    AppConfig.config = config
    val dbConnection = new DbConnection("source")
    val output = "jdbc:sqlserver://1.2.3.4:1234;database=dbTest;userName=mark;password=antony"
    dbConnection.getConnectionString should be (output)
  }

  "getConnectionString" should "return correct empty connection string for any other driver" in {
    val credentials = new Credentials(source = "source", driver = "randomDriver", server = "1.2.3.4", port = "1234",
      dbName = "dbTest", userName = "mark", password = "antony")
    val credentialsList = List(credentials)
    val config = new Config(applicationName = null, master = null, basePath = null, credentials = credentialsList)
    AppConfig.config = config
    val dbConnection = new DbConnection("source")
    dbConnection.getConnectionString should be ("")
  }

  "getDriver" should "return correct value of driver" in {
    val credentials = new Credentials(source = "source", driver = "randomDriver", server = "1.2.3.4", port = "1234",
      dbName = "dbTest", userName = "mark", password = "antony")
    val credentialsList = List(credentials)
    val config = new Config(applicationName = null, master = null, basePath = null, credentials = credentialsList)
    AppConfig.config = config
    val dbConnection = new DbConnection("source")
    dbConnection.getDriver should be ("randomDriver")
  }

}

