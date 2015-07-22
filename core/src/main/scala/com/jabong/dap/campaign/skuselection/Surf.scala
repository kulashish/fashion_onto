package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
 * two options:
 * 1. surf 1, 6, 2 - group by user
 * 2  surf 3
 *
 * 1. not bought yesterday
 * 2. past campaign check
 * 3. ref skus based on special price descending
 */
class Surf extends SkuSelector {
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???
}
