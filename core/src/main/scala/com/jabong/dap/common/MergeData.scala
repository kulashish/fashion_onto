package com.jabong.dap.common

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}

trait MergeData {

  def InsertUpdateMerge (dataFrame1: DataFrame, dataFrame2: DataFrame, primaryKey: String):RDD[Row]

}
