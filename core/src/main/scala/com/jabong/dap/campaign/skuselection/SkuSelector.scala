package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
Action Interface to perform certain actions like re-target,item on discount
  */
trait SkuSelector {

 // val hiveContext = Spark.getHiveContext()
  def execute(inDataFrame: DataFrame):DataFrame

  def skuFilter(inDataFrame:DataFrame):DataFrame


}
