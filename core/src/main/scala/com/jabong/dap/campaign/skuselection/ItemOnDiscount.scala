package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
 * Item On Discount Execution Class
 */
class ItemOnDiscount extends SkuSelector {

  // sku filter
  // 1. order should not have been placed for the ref sku yet
  // 2. Today's Special Price of SKU (SIMPLE – include size) is less than
  //      previous Special Price of SKU (when it was added to wishlist)
  // 3. This campaign shouldn’t have gone to the customer in the past 30 days for the same Ref SKU
  // 4. pick based on special price (descending)
  //
  // inDataFrame =  [(id_customer, sku, sku simple)]
  // itr30dayData = [(skusimple, date, special price)]
  override def skuFilter(inDataFrame: DataFrame, itr30dayData: DataFrame, campaignName: String): DataFrame = {
    null

  }

  // not needed
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???
}

