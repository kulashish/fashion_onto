package com.jabong.dap.data.storage.merge.common

import org.apache.spark.sql.DataFrame

trait MergeData {
  def InsertUpdateMerge(dataFrame1: DataFrame, dataFrame2: DataFrame, primaryKey: String): DataFrame
}
