package com.jabong.dap.data.acq.common

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.{ StringUtils, OptionUtils }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets

/**
 * Builds the path at which the requested data is to be saved.
 */

object PathBuilder {

  def getPath(tableInfo: TableInfo) = {
    val source = tableInfo.source
    var tableName = tableInfo.tableName
    if (source.equals(DataSets.CRM)) {
      //adding the replaceAll for the CRM 195 server tablenames.
      tableName = tableName.replaceAll("\\[dbo\\].\\[", "").replaceAll("\\]", "")
    } else if (source.equals(DataSets.ERP)) {
      //adding the replaceAll for the ERP server tablenames.
      tableName = tableName.replaceAll("\\[JADE\\].\\[dbo\\].\\[", "").replaceAll("\\]", "")
      tableName = StringUtils.cleanString(tableName)
    }
    val rangeStart = OptionUtils.getOptValue(tableInfo.rangeStart)
    val rangeEnd = OptionUtils.getOptValue(tableInfo.rangeEnd)

    tableInfo.mode match {
      case DataSets.FULL =>
        val dateNow = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)
        "%s/%s/%s/%s/%s".format(ConfigConstants.INPUT_PATH, source, tableName, DataSets.FULL, dateNow)
      case DataSets.MONTHLY_MODE =>
        val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
        val start = Calendar.getInstance()
        val end = Calendar.getInstance()
        start.setTime(format.parse(rangeStart))
        end.setTime(format.parse(rangeEnd))
        "%s/%s/%s/%s/%s/%s/%s"
          .format(ConfigConstants.INPUT_PATH, source, tableName, DataSets.MONTHLY_MODE, end.get(Calendar.YEAR), TimeUtils.withLeadingZeros(end.get(Calendar.MONTH) + 1),
            TimeUtils.withLeadingZeros(end.get(Calendar.DATE)))
      case DataSets.DAILY_MODE =>
        if (rangeStart == null && rangeEnd == null) {
          val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
          "%s/%s/%s/%s/%s".format(ConfigConstants.INPUT_PATH, source, tableName, DataSets.DAILY_MODE, dateYesterday)
        } else {
          val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(format.parse(rangeStart))
          end.setTime(format.parse(rangeEnd))
          "%s/%s/%s/%s/%s/%s/%s"
            .format(ConfigConstants.INPUT_PATH, source, tableName, DataSets.DAILY_MODE, end.get(Calendar.YEAR), TimeUtils.withLeadingZeros(end.get(Calendar.MONTH) + 1),
              TimeUtils.withLeadingZeros(end.get(Calendar.DATE)))
        }
      case DataSets.HOURLY_MODE =>
        if (rangeStart == null && rangeEnd == null) {
          val dateNow = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER)
          val hr = TimeUtils.withLeadingZeros(TimeUtils.getHour(null, TimeConstants.DATE_FORMAT) - 1)
          "%s/%s/%s/%s/%s/%s".format(ConfigConstants.INPUT_PATH, source, tableName, DataSets.HOURLY_MODE, dateNow, hr)
        } else {
          val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT)
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(format.parse(rangeStart))
          end.setTime(format.parse(rangeEnd))
          "%s/%s/%s/%s/%s/%s/%s/%s"
            .format(ConfigConstants.INPUT_PATH, source, tableName, DataSets.HOURLY_MODE, end.get(Calendar.YEAR), TimeUtils.withLeadingZeros(end.get(Calendar.MONTH) + 1),
              TimeUtils.withLeadingZeros(end.get(Calendar.DATE)), TimeUtils.withLeadingZeros(end.get(Calendar.HOUR_OF_DAY)))
        }
      case _ => ""
    }
  }

}
