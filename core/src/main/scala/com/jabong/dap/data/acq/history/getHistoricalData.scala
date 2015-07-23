package com.jabong.dap.data.acq.history

import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils }
import com.jabong.dap.data.acq.common.{ DbConnection, GetData, TableInfo }

/**
 * Created by pooja on 13/7/15.
 */
class getHistoricalData extends java.io.Serializable {
  def fetchData(tableInfo: TableInfo): Unit = {
    val dbConn = new DbConnection(tableInfo.source)
    val minDate = OptionUtils.getOptValue(tableInfo.rangeStart)

    val currMonthYear = TimeUtils.getMonthAndYear(null, TimeConstants.DATE_FORMAT)

    val monthYear = TimeUtils.getMonthAndYear(minDate, TimeConstants.DATE_FORMAT)

    for (yr <- monthYear.year to currMonthYear.year) {

      val startMonth = if (yr == monthYear.year) {
        monthYear.month + 1
      } else {
        1
      }

      val endMonth = if (yr == currMonthYear.year) {
        currMonthYear.month
      } else {
        12
      }

      for (mnth <- startMonth to endMonth) {
        val mnthStr = TimeUtils.withLeadingZeros(mnth)

        val start = yr.toString + "-" + mnthStr + "-01 " + TimeConstants.START_TIME

        val days = TimeUtils.getMaxDaysOfMonth(yr.toString + "-" + mnthStr + "-01", TimeConstants.DATE_FORMAT)
        val end = yr.toString + "-" + mnthStr + "-" + days + " " + TimeConstants.END_TIME

        val tblInfo = new TableInfo(source = tableInfo.source, tableName = tableInfo.tableName, primaryKey = tableInfo.primaryKey,
          mode = "monthly", saveFormat = tableInfo.saveFormat, saveMode = "ignore", dateColumn = tableInfo.dateColumn,
          rangeStart = Option.apply(start), rangeEnd = Option.apply(end), limit = tableInfo.limit, filterCondition = tableInfo.filterCondition,
          joinTables = tableInfo.joinTables)

        GetData.getData(dbConn, tblInfo)
      }
    }

    for (day <- 1 to currMonthYear.day - 1) {
      //      println("till date: " + (currMonthYear.day - 1))
      val mnthStr = TimeUtils.withLeadingZeros(currMonthYear.month + 1)
      val yrStr = currMonthYear.year.toString
      val start = yrStr + "-" + mnthStr + "-" + TimeUtils.withLeadingZeros(day) + " " + TimeConstants.START_TIME

      val end = yrStr + "-" + mnthStr + "-" + TimeUtils.withLeadingZeros(day) + " " + TimeConstants.END_TIME

      val tblInfo = new TableInfo(source = tableInfo.source, tableName = tableInfo.tableName, primaryKey = tableInfo.primaryKey,
        mode = "daily", saveFormat = tableInfo.saveFormat, saveMode = "ignore", dateColumn = tableInfo.dateColumn,
        rangeStart = Option.apply(start), rangeEnd = Option.apply(end), limit = tableInfo.limit, filterCondition = tableInfo.filterCondition,
        joinTables = tableInfo.joinTables)

      GetData.getData(dbConn, tblInfo)

    }
  }

}
