package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.data.acq.common.MergeJobConfig

/**
 * Runs the merge job for every json element in the merge job list.
 */
class Merger extends java.io.Serializable {
  def merge (): Unit = {
    val mergeMode = MergeJobConfig.mergeInfo.mergeMode

    mergeMode match {
      case "full" => MergeTables.mergeFull()
    }


  }



}
