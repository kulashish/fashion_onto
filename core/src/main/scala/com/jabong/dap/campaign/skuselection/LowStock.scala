package com.jabong.dap.campaign.skuselection

import org.apache.spark.sql.DataFrame

/**
  Low Stock Action Execution Class
 */
class LowStock extends SkuSelector{

  /*
Place Holder for Low Stock Action
 */
  override def execute(inputDataFrame: DataFrame): DataFrame = {
    return null
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???
}
