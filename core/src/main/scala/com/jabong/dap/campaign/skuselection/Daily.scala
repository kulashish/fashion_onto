package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, CustomerVariables, ProductVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

class Daily extends SkuSelector with Logging {

  // 1. pick based on special price (descending)
  // 1 day data
  // inDataFrame =  [(id_customer, sku simple)]
  // itrData = [(skusimple, date, stock, special price)]
  override def skuFilter(customerSkuData: DataFrame, yesterdayItrData: DataFrame): DataFrame = {
    if (customerSkuData == null || yesterdayItrData == null) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }

    val filteredSku = customerSkuData.join(yesterdayItrData, customerSkuData(ProductVariables.SKU_SIMPLE) === yesterdayItrData(ProductVariables.SKU_SIMPLE), "inner")
      .select(customerSkuData(CustomerVariables.FK_CUSTOMER), customerSkuData(ProductVariables.SKU_SIMPLE) as ProductVariables.SKU, yesterdayItrData(ProductVariables.SPECIAL_PRICE))

    logger.info("Join selected customer sku with sku data and get special price")
    //generate reference skus
    val refSkus = CampaignUtils.generateReferenceSkus(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return refSkus
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???
}
