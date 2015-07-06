package com.jabong.dap.data.storage.merge.common

import java.io.File

import com.jabong.dap.common.AppConfig
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.acq.common.MergeJobConfig

/**
 * Created by Abhay on 3/7/15.
 */
object PathBuilder {

  val basePath = AppConfig.config.basePath
  val mergeDate = MergeJobConfig.mergeInfo.mergeDate
  val source = MergeJobConfig.mergeInfo.source
  val tableName = MergeJobConfig.mergeInfo.tableName

  private def getDateDayBeforeYesterdayPath(): String = {
    if (mergeDate == null) {
      TimeUtils.getDayBeforeYesterdayDate().replaceAll("-", File.separator)
    } else {
      TimeUtils.getYesterdayDate(mergeDate).replaceAll("-", File.separator)
    }
  }

  private def getDateYesterdayDataPath(): String = {
    if (mergeDate == null) {
      TimeUtils.getYesterdayDate().replaceAll("-", File.separator)
    } else {
      mergeDate.replaceAll("-", File.separator)
    }
  }

  def getPathFullMerged(): String = {
    val dateDayBeforeYesterday = getDateDayBeforeYesterdayPath()
    "%s/%s/%s/full_merged/%s/".format(basePath, source, tableName, dateDayBeforeYesterday)
  }

  def getPathFull(): String = {
    val dateDayBeforeYesterday = getDateDayBeforeYesterdayPath()
    "%s/%s/%s/full/%s/".format(basePath, source, tableName, dateDayBeforeYesterday)
  }

  def getPathYesterdayData(): String = {
    val dateYesterday = getDateYesterdayDataPath()
    "%s/%s/%s/%s/".format(basePath, source, tableName, dateYesterday)
  }

  def getSavePathFullMerge(): String = {
    val dateYesterday = getDateYesterdayDataPath()
    "%s/%s/%s/full_merged/%s/".format(basePath, source, tableName, dateYesterday)
  }

}
