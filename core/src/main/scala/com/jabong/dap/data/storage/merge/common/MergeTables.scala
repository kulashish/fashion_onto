package com.jabong.dap.data.storage.merge.common

import java.io.File

import com.jabong.dap.common.{MergeUtils, Spark, AppConfig}
import com.jabong.dap.common.utils.Time
import com.jabong.dap.data.acq.common.MergeJobConfig

/**
 * Created by Abhay on 2/7/15.
 */
object MergeTables {
  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

  def mergeFull () = {
    val source = MergeJobConfig.mergeInfo.source
    val tableName = MergeJobConfig.mergeInfo.tableName
    val primaryKey = MergeJobConfig.mergeInfo.primaryKey
    val saveFormat = MergeJobConfig.mergeInfo.saveFormat
    val saveMode = MergeJobConfig.mergeInfo.saveMode
    val mergeMode = MergeJobConfig.mergeInfo.mergeMode

    val basePath = AppConfig.config.basePath

    val dateDayBeforeYesterday = Time.getDayBeforeYesterdayDate().replaceAll("-", File.separator)
    val dateYesterday = Time.getYesterdayDate().replaceAll("-", File.separator)

    val pathFullMerged = "%s/%s/%s/%s_merged/%s/".format(basePath, source, tableName, mergeMode, dateDayBeforeYesterday)
    lazy val pathFull = "%s/%s/%s/full/%s/".format(basePath, source, tableName, dateDayBeforeYesterday)
    lazy val pathYesterdayData = "%s/%s/%s/%s/".format(basePath, source, tableName, dateYesterday)

    val mergeBaseDataPath = if (DataVerifier.hdfsDataExists(pathFullMerged)) {
      pathFullMerged
    } else if (DataVerifier.hdfsDataExists(pathFull)) {
      pathFull
    } else {
      null
    }

    val mergeIncrementalDataPath = if (mergeBaseDataPath!= null && DataVerifier.hdfsDataExists(pathYesterdayData)) {
       pathYesterdayData
    } else {
      null
    }

    val context = getContext(saveFormat)

    val baseDF = context.read.format(saveFormat).load(mergeBaseDataPath)
    val incrementalDF = context.read.format(saveFormat).load(mergeIncrementalDataPath)
    val mergedDF = MergeUtils.InsertUpdateMerge(baseDF, incrementalDF, primaryKey)


    val savePath = "%s/%s/%s/%s_merged/%s/".format(basePath, source, tableName,mergeMode, dateYesterday)

    mergedDF.write.format(saveFormat).mode(saveMode).save(savePath)
  }

}
