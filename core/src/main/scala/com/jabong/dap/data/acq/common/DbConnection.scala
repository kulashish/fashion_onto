package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 9/6/15.
 */
case class DbConnection (source: String){
  require(source != null, "Source Type is null")
  var driver, server, port, dbName, userName, password = ""

  source match {
    case "erp" => {
      driver = "sqlserver"
      server = "103.29.235.143"
      port =  "1433"
      dbName = "JADE"
      userName = "report"
      password = "re@port"
    }
  }


  def getConnectionString =
  if (driver == "sqlserver") {
    "jdbc:sqlserver://%s:%s;database=%s;userName=%s;password=%s".format(server, port, dbName, userName, password)
  }
  else if (driver == "mysql"){
    "jdbc:mysql://%s:%s/%s?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&user=%s&password=%s".format(server, port, dbName, userName, password)
  }
  else {
    ""
  }


}
