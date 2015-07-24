package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.data.acq.common.MergeJobConfig
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging

/**
 * Runs the merge job for every json element in the merge job list.
 */
class Merger extends java.io.Serializable with Logging {
  def merge(isHistory: Boolean): Unit = {
    val mergeMode = MergeJobConfig.mergeInfo.mergeMode

    mergeMode match {
      case DataSets.FULL =>
        if (isHistory)
          MergeTables.mergeHistory(MergeJobConfig.mergeInfo)
        else
          MergeTables.merge(MergeJobConfig.mergeInfo)
      case DataSets.MONTHLY_MODE =>
        if (isHistory)
          MergeTables.mergeHistory(MergeJobConfig.mergeInfo)
        else
          MergeTables.merge(MergeJobConfig.mergeInfo)
      case _ => logger.error("Merge Mode: " + mergeMode + "not supported")
    }
  }
}
