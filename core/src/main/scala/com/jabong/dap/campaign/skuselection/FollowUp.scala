package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, CustomerVariables, ProductVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

class FollowUp extends SkuSelector with Logging {

  // 1. order should not have been placed for the ref sku yet
  // 2. pick based on special price (descending)
  // 1 day data
  // inDataFrame =  [(id_customer, sku, sku simple)]
  // itrData = [(skusimple, date, special price)]
  override def skuFilter(customerSkuData: DataFrame, itrData: DataFrame): DataFrame = {
    if (customerSkuData == null || itrData == null) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }

    val filteredSku = customerSkuData.join(itrData, customerSkuData(ProductVariables.SKU_SIMPLE) === itrData(ProductVariables.SKU_SIMPLE), "inner")
      .filter(ProductVariables.STOCK + " >= " + CampaignCommon.FOLLOW_UP_STOCK_VALUE)
      .select(customerSkuData(CustomerVariables.FK_CUSTOMER), customerSkuData(ProductVariables.SKU_SIMPLE) as ProductVariables.SKU, itrData(ProductVariables.SPECIAL_PRICE) as SalesOrderItemVariables.UNIT_PRICE)

    logger.info("Join selected customer sku with sku data and filter by stock>=" + CampaignCommon.FOLLOW_UP_STOCK_VALUE)
    //generate reference skus
    val refSkus = CampaignUtils.generateReferenceSkus(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return refSkus
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???
}
