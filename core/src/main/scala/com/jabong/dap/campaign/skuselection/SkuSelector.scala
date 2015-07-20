package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
 * Action Interface to perform certain actions like re-target,item on discount
 */
trait SkuSelector {

  def skuFilter(inDataFrame: DataFrame): DataFrame
  def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame
  def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame


}
