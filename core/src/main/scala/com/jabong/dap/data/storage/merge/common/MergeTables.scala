package com.jabong.dap.data.storage.merge.common

import java.io.File

import com.jabong.dap.common.time.{ Constants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.read.{ FormatResolver, ValidFormatNotFound }
import grizzled.slf4j.Logging

/**
 * Used to merge the data on the basis of the merge type.
 */

object MergeTables extends Logging {
  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

  def mergeFull(mergeInfo: MergeInfo) = {
    val primaryKey = mergeInfo.primaryKey
    val saveMode = mergeInfo.saveMode
    val source = mergeInfo.source
    val tableName = mergeInfo.tableName
    // If the incremental date is null than it is assumed that it will be yesterday's date.
    val incrDate = OptionUtils.getOptValue(mergeInfo.incrDate, TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER))
      .replaceAll("-", File.separator)

    // If incremental Data Mode is null then we assume that it will be "daily"
    val incrDataMode = OptionUtils.getOptValue(mergeInfo.incrMode, "daily")

    // If full Data date is null then we assume that it will be day before the Incremental Data's date.
    val fullDataDate = OptionUtils.getOptValue(mergeInfo.fullDate, TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER, incrDate))
      .replaceAll("-", File.separator)

    val pathFull = PathBuilder.getFullDataPath(fullDataDate, source, tableName)
    lazy val pathIncr = PathBuilder.getIncrDataPath(incrDate, incrDataMode, source, tableName)

    try {
      val saveFormat = FormatResolver.getFormat(pathFull)
      val context = getContext(saveFormat)

      val baseDF =
        context
          .read
          .format(saveFormat)
          .load(MergePathResolver.basePathResolver(pathFull))
      val incrementalDF =
        context
          .read
          .format(saveFormat)
          .load(MergePathResolver.incrementalPathResolver(pathIncr))
      val mergedDF = MergeUtils.InsertUpdateMerge(baseDF, incrementalDF, primaryKey)

      mergedDF.write.format(saveFormat).mode(saveMode).save(PathBuilder.getSavePathFullMerge(incrDate, source, tableName))
      println("merged " + pathIncr + " with " + pathFull)
    } catch {
      case e: DataNotFound =>
        logger.error("Data not at location: " + e.getMessage)
      case e: ValidFormatNotFound =>
        logger.error("Could not resolve format in which the data is saved")
    }
  }

  /**
   * This function gets called when we want to merge the historical data and get the latest full. This historical data
   * is assumed to be acquired using isHistory = true of Data acq. Here we get monthly till the last month and then
   * daily for the current month. After merging it will give us the final full till yesterday's date.
   * e.g., {
   *    "source": "bob",
   *    "tableName": "sales_order",
   *    "primaryKey": "id_sales_order",
   *    "mergeMode": "historical",
   *    "incrDate": "2012-06-30",
   *    "fullDate": "2012/05/31/24",
   *    "saveMode": "ignore"
   *  }
   * This will try to merge all the incremental data of each month starting from 2012-06 to 2015-06 (whichever is last
   * month) with the full data of date 2012/05/31. After this it will merge the daily data of the current month from 1st
   * of the month to yesterday's date.
   * @param mergeInfo - Merge Info object formed from the input mergeJson file.
   */
  def mergeHistory(mergeInfo: MergeInfo) = {
    var prevFullDate = OptionUtils.getOptValue(mergeInfo.fullDate)

    val currMonthYear = TimeUtils.getMonthAndYear(null, Constants.DATE_FORMAT)

    val minDate = OptionUtils.getOptValue(mergeInfo.incrDate)
    val monthYear = TimeUtils.getMonthAndYear(minDate, Constants.DATE_FORMAT)

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
        val days = TimeUtils.getMaxDaysOfMonth(yr.toString + "-" + mnthStr + "-01", Constants.DATE_FORMAT)
        val end = yr.toString + File.separator + mnthStr + File.separator + days

        val mrgInfo = new MergeInfo(source = mergeInfo.source, tableName = mergeInfo.tableName, primaryKey = mergeInfo.primaryKey, mergeMode = "full",
          incrDate = Option.apply(end), fullDate = Option.apply(prevFullDate), incrMode = Option.apply("monthly"), saveMode = "ignore")

        mergeFull(mrgInfo)

        prevFullDate = end + File.separator + "24"
      }
    }

    for (day <- 1 to currMonthYear.day - 1) {
      val mnthStr = TimeUtils.withLeadingZeros(currMonthYear.month + 1)
      val yrStr = currMonthYear.year.toString
      val end = yrStr + File.separator + mnthStr + File.separator + TimeUtils.withLeadingZeros(day)

      val mrgInfo = new MergeInfo(source = mergeInfo.source, tableName = mergeInfo.tableName, primaryKey = mergeInfo.primaryKey, mergeMode = "full",
        incrDate = Option.apply(end), fullDate = Option.apply(prevFullDate), incrMode = Option.apply("daily"), saveMode = "ignore")

      mergeFull(mrgInfo)

      prevFullDate = end + File.separator + "24"

    }
  }

}
