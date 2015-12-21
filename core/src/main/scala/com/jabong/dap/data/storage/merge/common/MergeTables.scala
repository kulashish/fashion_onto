package com.jabong.dap.data.storage.merge.common

import java.io.File

import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.read.{ FormatResolver, ValidFormatNotFound }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.PathBuilder.DataNotExist
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Used to merge the data on the basis of the merge type.
 */

object MergeTables extends Logging {

  def merge(mergeInfo: MergeInfo): Unit = {
    val primaryKey = mergeInfo.primaryKey
    val saveMode = mergeInfo.saveMode
    val source = mergeInfo.source
    val tableName = mergeInfo.tableName
    val mergeMode = mergeInfo.mergeMode
    var mergedDF: DataFrame = null

    // If the incremental date is null than it is assumed that it will be yesterday's date.
    val incrDate = OptionUtils.getOptValue(mergeInfo.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
      .replaceAll("-", File.separator)

    val savePath = PathBuilder.getSavePathMerge(source, tableName, mergeMode, incrDate)

    if (!DataWriter.canWrite(saveMode, savePath)) {
      return
    }

    // If incremental Data Mode is null then we assume that it will be "daily"
    val incrDataMode = OptionUtils.getOptValue(mergeInfo.incrMode, DataSets.DAILY_MODE)

    // If full Data date is null then we assume that it will be day before the Incremental Data's date.
    val prevDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
    val prevDataDate = OptionUtils.getOptValue(mergeInfo.fullDate, prevDate).replaceAll("-", File.separator)

    try {
      val pathPrevData = PathBuilder.getPrevDataPath(source, tableName, mergeMode, prevDataDate)
      lazy val pathIncr = PathBuilder.getIncrDataPath(incrDate, incrDataMode, source, tableName)

      val saveFormat = FormatResolver.getFormat(pathPrevData)
      val context = Spark.getContext(saveFormat)

      val baseDF =
        context
          .read
          .format(saveFormat)
          .load(MergePathResolver.basePathResolver(pathPrevData))

      val incrementalDF =
        context
          .read
          .format(saveFormat)
          .load(MergePathResolver.incrementalPathResolver(pathIncr))

      if(primaryKey != null && primaryKey.length > 1)
        mergedDF = MergeUtils.InsertUpdateMerge(baseDF, incrementalDF, primaryKey)
      else
        mergedDF = MergeUtils.InsertUpdateMerge(baseDF, incrementalDF, primaryKey(0))

      println("merged " + pathIncr + " with " + pathPrevData)

      var filteredDF = mergedDF

      if (DataSets.MONTHLY_MODE.equals(mergeMode)) {

        val colName = OptionUtils.getOptValue(mergeInfo.dateColumn)

        val before30Days = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
        val time = before30Days.replaceAll(File.separator, "-") + " " + TimeConstants.END_TIME_MS

        filteredDF = mergedDF.filter(colName + " > " + "'" + time + "'")
        println("filtered and got records > " + time)
      }

      filteredDF.write.format(saveFormat).mode(saveMode).save(savePath)
      println("Successfully written data to " + savePath)
      context.clearCache()
    } catch {
      case e: DataNotFound =>
        logger.error("Data not at location: " + e.getMessage)
      case e: ValidFormatNotFound =>
        logger.error("Could not resolve format in which the data is saved")
      case e: DataNotExist =>
        logger.error("Data in the given Path doesn't exist")
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
    println("Inside History Code")

    var prevDate = OptionUtils.getOptValue(mergeInfo.fullDate)
    println(prevDate)

    val currMonthYear = TimeUtils.getMonthAndYear(null, TimeConstants.DATE_FORMAT)

    val minDate = OptionUtils.getOptValue(mergeInfo.incrDate)
    println(minDate)
    val monthYear = TimeUtils.getMonthAndYear(minDate, TimeConstants.DATE_FORMAT)

    // no need of Monthly data as we are assuming here that we got last month's data already.
    // Hence this case to be run only in case of FULL data merge.
    if (DataSets.FULL.equals(mergeInfo.mergeMode)) {

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
          val days = TimeUtils.getMaxDaysOfMonth(yr.toString + "-" + mnthStr + "-01", TimeConstants.DATE_FORMAT)
          val end = yr.toString + File.separator + mnthStr + File.separator + days

          val mrgInfo = new MergeInfo(source = mergeInfo.source, tableName = mergeInfo.tableName,
            primaryKey = mergeInfo.primaryKey, mergeMode = mergeInfo.mergeMode, dateColumn = mergeInfo.dateColumn,
            incrDate = Option.apply(end), fullDate = Option.apply(prevDate),
            incrMode = Option.apply(DataSets.MONTHLY_MODE), saveMode = DataSets.IGNORE_SAVEMODE)

          merge(mrgInfo)
          prevDate = end
        }
      }
    }

    for (day <- 1 to currMonthYear.day - 1) {
      println("starting daywise merge")
      val mnthStr = TimeUtils.withLeadingZeros(currMonthYear.month + 1)
      val yrStr = currMonthYear.year.toString
      val end = yrStr + File.separator + mnthStr + File.separator + TimeUtils.withLeadingZeros(day)

      println("Increment Date: " + end)
      println("prevDate: " + prevDate)

      val mrgInfo = new MergeInfo(source = mergeInfo.source, tableName = mergeInfo.tableName,
        primaryKey = mergeInfo.primaryKey, mergeMode = mergeInfo.mergeMode, dateColumn = mergeInfo.dateColumn,
        incrDate = Option.apply(end), fullDate = Option.apply(prevDate),
        incrMode = Option.apply(DataSets.DAILY_MODE), saveMode = DataSets.IGNORE_SAVEMODE)

      merge(mrgInfo)

      prevDate = end

    }
  }

}
