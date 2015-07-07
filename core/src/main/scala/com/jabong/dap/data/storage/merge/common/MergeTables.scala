package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.Spark
import com.jabong.dap.common.merge.MergeUtils
import com.jabong.dap.data.acq.common.MergeJobConfig

/**
 * Used to merge the data on the basis of the merge type.
 */

object MergeTables {
  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

  def mergeFull() = {
    val primaryKey = MergeJobConfig.mergeInfo.primaryKey
    val saveFormat = MergeJobConfig.mergeInfo.saveFormat
    val saveMode = MergeJobConfig.mergeInfo.saveMode

    val pathFullMerged = PathBuilder.getPathFullMerged()
    lazy val pathFull = PathBuilder.getPathFull()
    lazy val pathYesterdayData = PathBuilder.getPathYesterdayData()

    val mergeBaseDataPath = if (DataVerifier.hdfsDataExists(pathFullMerged)) {
      pathFullMerged
    } else if (DataVerifier.hdfsDataExists(pathFull)) {
      pathFull
    } else {
      null
    }

    val mergeIncrementalDataPath = if (mergeBaseDataPath != null && DataVerifier.hdfsDataExists(pathYesterdayData)) {
      pathYesterdayData
    } else {
      null
    }

    val context = getContext(saveFormat)

    val baseDF = context.read.format(saveFormat).load(mergeBaseDataPath)
    val incrementalDF = context.read.format(saveFormat).load(mergeIncrementalDataPath)
    val mergedDF = MergeUtils.InsertUpdateMerge(baseDF, incrementalDF, primaryKey)

    val savePath = PathBuilder.getSavePathFullMerge()

    mergedDF.write.format(saveFormat).mode(saveMode).save(savePath)
  }

}
