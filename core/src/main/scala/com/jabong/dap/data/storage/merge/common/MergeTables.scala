package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.Spark
import com.jabong.dap.data.acq.common.MergeJobConfig
import com.jabong.dap.data.read.{ ValidFormatNotFound, FormatResolver }
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

  def mergeFull() = {
    val primaryKey = MergeJobConfig.mergeInfo.primaryKey
    val saveMode = MergeJobConfig.mergeInfo.saveMode

    val pathFull = PathBuilder.getPathFull
    lazy val pathYesterdayData = PathBuilder.getPathYesterdayData

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
          .load(MergePathResolver.incrementalPathResolver(pathYesterdayData))
      val mergedDF = MergeUtils.InsertUpdateMerge(baseDF, incrementalDF, primaryKey)

      mergedDF.write.format(saveFormat).mode(saveMode).save(PathBuilder.getSavePathFullMerge)
    } catch {
      case e: DataNotFound =>
        logger.error("Data not at location: " + e.getMessage)
      case e: ValidFormatNotFound =>
        logger.error("Could not resolve format in which the data is saved")
    }
  }

}
