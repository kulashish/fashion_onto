package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{CustomerVariables, ProductVariables}
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object Daily extends Logging {

  // 1. pick based on special price (descending)
  // 1 day data
  // inDataFrame =  [(id_customer, sku simple)]
  // itrData = [(skusimple, date, stock, special price)]
  def skuFilter(customerSkuData: DataFrame, yesterdayItrData: DataFrame): DataFrame = {
    if (null == customerSkuData || null == yesterdayItrData) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }
    customerSkuData.printSchema()
    yesterdayItrData.printSchema()

    val filteredSku = customerSkuData.join(yesterdayItrData, customerSkuData(ProductVariables.SKU_SIMPLE) === yesterdayItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(
        customerSkuData(CustomerVariables.FK_CUSTOMER),
        customerSkuData(CustomerVariables.EMAIL),
        customerSkuData(ProductVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
        yesterdayItrData(ProductVariables.SPECIAL_PRICE),
        yesterdayItrData(ProductVariables.BRICK),
        yesterdayItrData(ProductVariables.BRAND),
        yesterdayItrData(ProductVariables.MVP),
        yesterdayItrData(ProductVariables.GENDER),
        yesterdayItrData(ProductVariables.PRODUCT_NAME))

    logger.info("Join selected customer sku with sku data and get special price")
    //generate reference skus
    // val refSkus = CampaignUtils.generateReferenceSku(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    filteredSku
  }
}
