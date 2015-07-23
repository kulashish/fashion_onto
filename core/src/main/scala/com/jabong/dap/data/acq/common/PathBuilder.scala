package com.jabong.dap.data.acq.common

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets

/**
 * Builds the path at which the requested data is to be saved.
 */

object PathBuilder {

  def getPath(tableInfo: TableInfo) = {
    val source = tableInfo.source
    val tableName = tableInfo.tableName
    val rangeStart = OptionUtils.getOptValue(tableInfo.rangeStart)
    val rangeEnd = OptionUtils.getOptValue(tableInfo.rangeEnd)

    tableInfo.mode match {
      case DataSets.FULL =>
        val dateNow = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)
        "%s/%s/%s/%s/%s".format(DataSets.INPUT_PATH, source, tableName, DataSets.FULL, dateNow)
      case DataSets.MONTHLY_MODE =>
        val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
        val start = Calendar.getInstance()
        val end = Calendar.getInstance()
        start.setTime(format.parse(rangeStart))
        end.setTime(format.parse(rangeEnd))
        "%s/%s/%s/%s/%s/%s/%s"
          .format(DataSets.INPUT_PATH, source, tableName, DataSets.MONTHLY_MODE, end.get(Calendar.YEAR), TimeUtils.withLeadingZeros(end.get(Calendar.MONTH) + 1),
            TimeUtils.withLeadingZeros(end.get(Calendar.DATE)))
      case DataSets.DAILY_MODE =>
        if (rangeStart == null && rangeEnd == null) {
          val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
          "%s/%s/%s/%s/%s".format(DataSets.INPUT_PATH, source, tableName, DataSets.DAILY_MODE, dateYesterday)
        } else {
          val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(format.parse(rangeStart))
          end.setTime(format.parse(rangeEnd))
          "%s/%s/%s/%s/%s/%s/%s"
            .format(DataSets.INPUT_PATH, source, tableName, DataSets.DAILY_MODE, end.get(Calendar.YEAR), TimeUtils.withLeadingZeros(end.get(Calendar.MONTH) + 1),
              TimeUtils.withLeadingZeros(end.get(Calendar.DATE)))
        }
      case DataSets.HOURLY_MODE =>
        val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
        val start = Calendar.getInstance()
        val end = Calendar.getInstance()
        start.setTime(format.parse(rangeStart))
        end.setTime(format.parse(rangeEnd))
        "%s/%s/%s/%s/%s/%s/%s/%s"
          .format(DataSets.INPUT_PATH, source, tableName, DataSets.HOURLY_MODE, end.get(Calendar.YEAR), TimeUtils.withLeadingZeros(end.get(Calendar.MONTH) + 1),
            TimeUtils.withLeadingZeros(end.get(Calendar.DATE)), TimeUtils.withLeadingZeros(end.get(Calendar.HOUR_OF_DAY)))
      case _ => ""
    }
  }

}
