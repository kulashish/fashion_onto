package com.jabong.dap.data.acq.common

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets

/**
 * Builds the condition for the query to fetch the data and get the min and max values of primary key.
 */
object ConditionBuilder {
  def getCondition(driver: String, tableInfo: TableInfo): String = {

    val mode = tableInfo.mode
    val dateColumn = OptionUtils.getOptValue(tableInfo.dateColumn)
    val rangeStart = OptionUtils.getOptValue(tableInfo.rangeStart)
    val rangeEnd = OptionUtils.getOptValue(tableInfo.rangeEnd)
    val filterCondition = OptionUtils.getOptValue(tableInfo.filterCondition)
    val source = tableInfo.source
    val tempFilterCondition = if (filterCondition == null) {
      ""
    } else {
      "AND %s".format(filterCondition)
    }

    if (null == rangeStart && null == rangeEnd && DataSets.DAILY_MODE == mode) {
      var startTime = TimeConstants.START_TIME
      var endTime = TimeConstants.END_TIME
      if (DataSets.SQLSERVER.equals(driver)) {
        startTime = TimeConstants.START_TIME + ".0"
        endTime = TimeConstants.END_TIME + ".9"
      }
      val prevDayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)
      "WHERE t1.%s >= '%s %s' AND t1.%s <= '%s %s' %s".format(dateColumn, prevDayDate, startTime,
        dateColumn, prevDayDate, endTime, tempFilterCondition)
    } else if (null == rangeStart && null == rangeEnd && DataSets.HOURLY_MODE == mode) {
      var startMin = TimeConstants.START_MIN
      var endMin = TimeConstants.END_MIN
      if (DataSets.SQLSERVER.equals(driver)) {
        startMin = TimeConstants.START_MIN + ".0"
        endMin = TimeConstants.END_MIN + ".9"
      }
      val dateNow = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT)
      val hr = TimeUtils.withLeadingZeros(TimeUtils.getHour(null, TimeConstants.DATE_FORMAT) - 1)
      "WHERE t1.%s >= '%s %s:%s' AND t1.%s <= '%s %s:%s' %s".format(dateColumn, dateNow, hr, startMin,
        dateColumn, dateNow, hr, endMin, tempFilterCondition)
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
