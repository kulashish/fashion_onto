package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

class FollowUp extends SkuSelector{

  // 1. order should not have been placed for the ref sku yet
  // 2. pick based on special price (descending)
  // 1 day data
  // inDataFrame =  [(id_customer, sku, sku simple)]
  // itr30dayData = [(skusimple, date, special price)]
  override def skuFilter(inDataFrame: DataFrame, itrData: DataFrame, campaignName: String): DataFrame = {
    null
  }
  
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???
}
