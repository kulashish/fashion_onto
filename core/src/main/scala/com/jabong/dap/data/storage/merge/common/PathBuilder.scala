package com.jabong.dap.data.storage.merge.common

import java.io.File

import com.jabong.dap.common.{ OptionUtils, AppConfig }
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.acq.common.MergeJobConfig
import com.jabong.dap.common.time.Constants

/**
 * Builds the path for the input data for creating the dataFrames and
 * the path at which the data is to be saved.
 */
object PathBuilder {

  val basePath = AppConfig.config.basePath
  val mergeDate = OptionUtils.getOptValue(MergeJobConfig.mergeInfo.mergeDate)
  val source = MergeJobConfig.mergeInfo.source
  val tableName = MergeJobConfig.mergeInfo.tableName

  private def getDateDayBeforeYesterdayPath: String = {
    if (null == mergeDate) {
      TimeUtils.getDateAfterNDays(-2, Constants.DATE_FORMAT_FOLDER)
    } else {
      TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER, mergeDate)
    }
  }

  private def getDateYesterdayDataPath: String = {
    if (null == mergeDate) {
      TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER)
    } else {
      mergeDate.replaceAll("-", File.separator)
    }
  }

  def getPathFull: String = {
    val dateDayBeforeYesterday = getDateDayBeforeYesterdayPath
    "%s/%s/%s/full/%s".format(basePath, source, tableName, dateDayBeforeYesterday)
  }

  def getPathYesterdayData: String = {
    val dateYesterday = getDateYesterdayDataPath
    "%s/%s/%s/daily/%s".format(basePath, source, tableName, dateYesterday)
  }

  def getSavePathFullMerge: String = {
    val dateYesterday = getDateYesterdayDataPath
    "%s/%s/%s/full/%s".format(basePath, source, tableName, dateYesterday)
  }

}
