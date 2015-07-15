package com.jabong.dap.data.acq.common

import com.jabong.dap.common.time.{ Constants, TimeUtils }

/**
 * Builds the condition for the query to fetch the data and get the min and max values of primary key.
 */
object ConditionBuilder {
  def getCondition(): String = {

    val mode = AcqImportInfo.tableInfo.mode
    val dateColumn = AcqImportInfo.tableInfo.dateColumn
    val rangeStart = AcqImportInfo.tableInfo.rangeStart
    val rangeEnd = AcqImportInfo.tableInfo.rangeEnd
    val filterCondition = AcqImportInfo.tableInfo.filterCondition
    val tempFilterCondition = if (filterCondition == null) {
      ""
    } else {
      "AND %s".format(filterCondition)
    }

    if (rangeStart == null && rangeEnd == null && mode == "daily") {
      val prevDayDate = TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT)
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
