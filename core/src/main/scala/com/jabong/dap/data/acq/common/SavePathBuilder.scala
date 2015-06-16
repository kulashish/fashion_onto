package com.jabong.dap.data.acq.common

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.Constants
import com.jabong.dap.common.utils.Time

/**
 * Created by Abhay on 16/6/15.
 */
object SavePathBuilder {

  def getSavePath (mode: String, source: String, tableName: String, rangeStart: String, rangeEnd: String) ={
    val basePath = "/home/test/sparkData"
    if ( mode == "full") {
      val dateNow = Time.getTodayDateWithHrs().replaceAll("-", File.separator)
      "%s/%s/%s/full/%s/".format(basePath, source, tableName, dateNow)
    } else if ( mode == "daily" && rangeStart == null && rangeEnd == null) {
      val dateYesterday = Time.getYesterdayDate().replaceAll("-", File.separator)
      "%s/%s/%s/%s/".format(basePath, source, tableName, dateYesterday)
    } else if ( mode == "daily") {
      val format = new SimpleDateFormat(Constants.DATETIME_FORMAT)
      val start = Calendar.getInstance()
      val end = Calendar.getInstance()
      start.setTime(format.parse(rangeStart))
      end.setTime(format.parse(rangeEnd))
      "%s/%s/%s/%s/%s/%s_%s".format(basePath, source, tableName, start.get(Calendar.YEAR), withLeadingZeros(start.get(Calendar.MONTH)),
        withLeadingZeros(start.get(Calendar.DATE)), withLeadingZeros(end.get(Calendar.DATE)))
    } else if (mode == "hourly"){
      val format = new SimpleDateFormat(Constants.DATETIME_FORMAT)
      val start = Calendar.getInstance()
      val end = Calendar.getInstance()
      start.setTime(format.parse(rangeStart))
      end.setTime(format.parse(rangeEnd))
      println(start)
      println(end)
      "%s/%s/%s/%s/%s/%s/%s_%s".format(basePath, source, tableName, start.get(Calendar.YEAR), withLeadingZeros(start.get(Calendar.MONTH)),
        withLeadingZeros(start.get(Calendar.DATE)), withLeadingZeros(start.get(Calendar.HOUR_OF_DAY)), withLeadingZeros(end.get(Calendar.HOUR_OF_DAY)))
    } else {
      ""
    }


  }

  def withLeadingZeros (input: Int) :String = {
    if (input < 10) {
      "0%s".format(input)
    } else {
      "%s".format(input)
    }
  }

}
