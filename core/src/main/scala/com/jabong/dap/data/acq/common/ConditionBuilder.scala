package com.jabong.dap.data.acq.common

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets

/**
 * Builds the condition for the query to fetch the data and get the min and max values of primary key.
 */
object ConditionBuilder {
  def getCondition(tableInfo: TableInfo): String = {

    val mode = tableInfo.mode
    val dateColumn = OptionUtils.getOptValue(tableInfo.dateColumn)
    val rangeStart = OptionUtils.getOptValue(tableInfo.rangeStart)
    val rangeEnd = OptionUtils.getOptValue(tableInfo.rangeEnd)
    val filterCondition = OptionUtils.getOptValue(tableInfo.filterCondition)
    val tempFilterCondition = if (filterCondition == null) {
      ""
    } else {
      "AND %s".format(filterCondition)
    }

    if (rangeStart == null && rangeEnd == null && mode == DataSets.DAILY_MODE) {
      val prevDayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)
      "WHERE t1.%s >= '%s %s' AND t1.%s <= '%s %s' %s".format(dateColumn, prevDayDate, TimeConstants.START_TIME,
        dateColumn, prevDayDate, TimeConstants.END_TIME, tempFilterCondition)
    } else if (mode == DataSets.FULL && filterCondition == null) {
      ""
    } else if (mode == DataSets.FULL && filterCondition != null) {
      "WHERE %s".format(filterCondition)
    } else if (mode == DataSets.HOURLY_MODE || (mode == DataSets.DAILY_MODE && rangeStart != null && rangeEnd != null)
      || (mode == DataSets.MONTHLY_MODE && rangeStart != null && rangeEnd != null)) {
      "WHERE t1.%s >= '%s' AND t1.%s <= '%s' %s".format(dateColumn, rangeStart, dateColumn, rangeEnd,
        tempFilterCondition)
    } else {
      ""
    }
  }
}

