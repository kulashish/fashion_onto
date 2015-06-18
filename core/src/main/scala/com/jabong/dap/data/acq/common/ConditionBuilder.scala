package com.jabong.dap.data.acq.common

import com.jabong.dap.common.utils.Time

/**
 * Created by Abhay on 9/6/15.
 */
object ConditionBuilder {
  def getCondition(mode: String, dateColumn: String, rangeStart: String, rangeEnd: String, filterCondition: String): String = {
    if (filterCondition == null) {
      if (rangeStart == null && rangeEnd == null && mode == "daily") {
        val prevDayDate = Time.getYesterdayDate()
        "WHERE %s >= '%s 00:00:00' AND %s <= '%s 23:59:59'".format(dateColumn, prevDayDate, dateColumn, prevDayDate)
      } else if (mode == "full") {
        ""
      } else {
        "WHERE %s >= '%s' AND %s <= '%s'".format(dateColumn, rangeStart, dateColumn, rangeEnd)
      }
    } else {
      if (rangeStart == null && rangeEnd == null && mode == "daily") {
        val prevDayDate = Time.getYesterdayDate()
        "WHERE %s >= '%s 00:00:00' AND %s <= '%s 23:59:59' AND %s".format(dateColumn, prevDayDate, dateColumn, prevDayDate, filterCondition)
      } else if (mode == "full") {
        "WHERE %s".format(filterCondition)
      } else {
        "WHERE %s >= '%s' AND %s <= '%s' AND %s".format(dateColumn, rangeStart, dateColumn, rangeEnd, filterCondition)
      }
    }
  }
}

