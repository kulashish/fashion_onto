package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.data.acq.common.MergeJobConfig

/**
 * Created by Abhay on 2/7/15.
 */
class Merger extends java.io.Serializable {
  def merge (): Unit = {
    val mergeMode = MergeJobConfig.mergeInfo.mergeMode

    mergeMode match {
      case "full" => MergeTables.mergeFull()
    }


  }



}
