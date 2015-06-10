package com.jabong.dap.data.acq.common

/**
 * Created by Abhay on 9/6/15.
 */
object ConditionBuilder {
  def getCondition (mode: String, rangeStart: String, rangeEnd: String, dateColumn: String, tablePrimaryKey: String) : String = {

    if (mode == "full") {
      ""
    }
    else if (mode == "limit") {
      "WHERE %s >= %s  AND %s <= %s".format(tablePrimaryKey, rangeStart, tablePrimaryKey, rangeEnd)
    }
    else if (mode == "range" ){
      "WHERE %s >= %s 00:00:00 AND %s <= %s 23:59:59". format(dateColumn, rangeStart, dateColumn, rangeEnd)
    }
    else {
      ""
    }
  }

}
