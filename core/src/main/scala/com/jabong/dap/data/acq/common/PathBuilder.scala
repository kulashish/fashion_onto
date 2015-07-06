package com.jabong.dap.data.acq.common

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.AppConfig
import com.jabong.dap.common.time.{ Constants, TimeUtils }

/**
 * Builds the path at which the requested data is to be saved.
 */

object PathBuilder {

  def getPath() = {
    val basePath = AppConfig.config.basePath
    val source = AcqImportInfo.tableInfo.source
    val tableName = AcqImportInfo.tableInfo.tableName
    val rangeStart = AcqImportInfo.tableInfo.rangeStart
    val rangeEnd = AcqImportInfo.tableInfo.rangeEnd

    AcqImportInfo.tableInfo.mode match {
      case "full" =>
        val dateNow = TimeUtils.getTodayDate(Constants.DATE_TIME_FORMAT_HRS_FOLDER)
        "%s/%s/%s/full/%s/".format(basePath, source, tableName, dateNow)
      case "daily" =>
        if (rangeStart == null && rangeEnd == null) {
          val dateYesterday = TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER)
          "%s/%s/%s/%s/".format(basePath, source, tableName, dateYesterday)
        } else {
          val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
          val start = Calendar.getInstance()
          val end = Calendar.getInstance()
          start.setTime(format.parse(rangeStart))
          end.setTime(format.parse(rangeEnd))
          "%s/%s/%s/%s/%s/%s_%s"
            .format(basePath, source, tableName, start.get(Calendar.YEAR), withLeadingZeros(start.get(Calendar.MONTH) + 1),
              withLeadingZeros(start.get(Calendar.DATE)), withLeadingZeros(end.get(Calendar.DATE)))
        }
      case "hourly" =>
        val format = new SimpleDateFormat(Constants.DATE_TIME_FORMAT)
        val start = Calendar.getInstance()
        val end = Calendar.getInstance()
        start.setTime(format.parse(rangeStart))
        end.setTime(format.parse(rangeEnd))
        "%s/%s/%s/%s/%s/%s/%s_%s"
          .format(basePath, source, tableName, start.get(Calendar.YEAR), withLeadingZeros(start.get(Calendar.MONTH) + 1),
            withLeadingZeros(start.get(Calendar.DATE)), withLeadingZeros(start.get(Calendar.HOUR_OF_DAY)),
            withLeadingZeros(end.get(Calendar.HOUR_OF_DAY)))
      case _ => ""
    }
  }

  /**
   * Converts integer containing day or month of date to a string with the format MM or dd, respectively.
   */
  def withLeadingZeros(input: Int): String = {
    if (input < 10) {
      "0%s".format(input)
    } else {
      "%s".format(input)
    }
  }

}
