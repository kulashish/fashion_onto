package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
 * 1. order should not have been placed for the ref sku yet
 * 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
 * 3. pick n ref based on special price (descending)
 * 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU
 */
class LowStock extends SkuSelector {
  
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  // input will be [(id_customer, sku, sku simple)]
  // case 1: only sku simple: price not available (need to query itr)
  // case 2: only sku simple + special price available
  // case 3: sku simple and skus both: price not available
  override def skuFilter(inDataFrame: DataFrame, itrDataFrame: DataFrame, campaignName: String): DataFrame = {
    null
  }
  
}
