package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
  Item On Discount Execution Class
 */
class ItemOnDiscount extends SkuSelector{
  /*
Place Holder for Item on Discount Action
 */
  override def execute(inputDataFrame: DataFrame): DataFrame = {
    return null
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???
}

