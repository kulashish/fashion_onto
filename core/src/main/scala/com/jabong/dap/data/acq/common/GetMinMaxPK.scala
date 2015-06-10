package com.jabong.dap.data.acq.common


/**
 * Created by Abhay on 9/6/15.
 */
object GetMinMaxPK {
  def getMinMax(dbconn: DbConnection, tableName: String, condition: String, tablePrimaryKey:  String) : MinMax = {
    var minMax = new MinMax(0,0)
    val minMaxQuery = "SELECT MIN(%s), MAX(%s) FROM %s %s".format(tablePrimaryKey, tablePrimaryKey, tableName, condition)
    println(minMaxQuery)          //  test for query
    return  minMax




  }
}
