package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, CustomerVariables, SalesOrderVariables, ProductVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object Daily extends Logging {

  // 1. pick based on special price (descending)
  // 1 day data
  // inDataFrame =  [(id_customer, sku simple)]
  // itrData = [(skusimple, date, stock, special price)]
  def skuFilter(customerSkuData: DataFrame, yesterdayItrData: DataFrame): DataFrame = {
    if (customerSkuData == null || yesterdayItrData == null) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }

    val filteredSku = customerSkuData.join(yesterdayItrData, customerSkuData(ProductVariables.SKU_SIMPLE) === yesterdayItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(
        customerSkuData("*"),
        yesterdayItrData("*"))
      .drop(yesterdayItrData(ProductVariables.SKU_SIMPLE))
      .drop(yesterdayItrData(ProductVariables.CREATED_AT))

    logger.info("Join selected customer sku with sku data and get special price")
    //generate reference skus
    // val refSkus = CampaignUtils.generateReferenceSku(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return filteredSku
  }
}
