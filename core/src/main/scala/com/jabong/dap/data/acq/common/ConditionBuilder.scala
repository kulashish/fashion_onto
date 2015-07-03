package com.jabong.dap.data.acq.common

import com.jabong.dap.common.time.TimeUtils

/**
 * Created by Abhay on 9/6/15.
 */
object ConditionBuilder {
  def getCondition(mode: String, dateColumn: String, rangeStart: String, rangeEnd: String,
    filterCondition: String): String = {

    val tempFilterCondition = if (filterCondition == null) {
      ""
    } else {
      "AND %s".format(filterCondition)
    }

    if (rangeStart == null && rangeEnd == null && mode == "daily") {
      val prevDayDate = TimeUtils.getYesterdayDate()
      "WHERE t1.%s >= '%s 00:00:00' AND t1.%s <= '%s 23:59:59' %s".format(dateColumn, prevDayDate, dateColumn,
        prevDayDate, tempFilterCondition)
    } else if (mode == "full" && filterCondition == null) {
      ""
    } else if (mode == "full" && filterCondition != null) {
      "WHERE %s".format(filterCondition)
    } else if (mode == "hourly" || (mode == "daily" && rangeStart != null && rangeEnd != null)) {
      "WHERE t1.%s >= '%s' AND t1.%s <= '%s' %s".format(dateColumn, rangeStart, dateColumn, rangeEnd,
        tempFilterCondition)
    } else {
      ""
    }
  }
}

