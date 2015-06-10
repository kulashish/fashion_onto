package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 9/6/15.
 */
case class DbConnection (driver: String, server: String, port: String, dbName: String, name: String, pw: String){
  require(driver != null, "Connection Type is null")
  require(server != null, "DB Server parameter is null")
  require(port != null, "Port parameter is null")
  require(dbName != null, "DB Name parameter is null")
  require(name != null, "DB (user) name parameter is null")
  require(pw != null, "DB pw parameter is null")

  def getConnectionString =
  if (driver == "sqlserver") {
    "jdbc:sqlserver://%s:%s;database=%s;userName=%s;password=%s".format(server, port, dbName, name, pw)
  }
  else if (driver == "mysql"){
    "jdbc:mysql://%s:%s/%s?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&user=%s&password=%s".format(server, port, dbName, name, pw)
  }
  else {
    ""
  }


}
