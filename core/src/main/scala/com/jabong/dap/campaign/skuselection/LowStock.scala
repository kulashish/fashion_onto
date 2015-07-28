package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
 * 3. pick n ref based on special price (descending)
 * 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU
 */
class LowStock extends SkuSelector with Logging {

  // input will be [(id_customer, sku simple)] or [(id_customer, sku)]
  // case 1: only sku simple
  // case 2: only sku
  override def skuFilter(customerSkuData: DataFrame, itrDataFrame: DataFrame): DataFrame = {
    if (customerSkuData == null || itrDataFrame == null) {
      return null
    }

    val filteredSku = customerSkuData.join(
      itrDataFrame, customerSkuData(ProductVariables.SKU_SIMPLE) === itrDataFrame(ProductVariables.SKU_SIMPLE), "inner")
      .filter(ProductVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)
      .select(customerSkuData(CustomerVariables.FK_CUSTOMER),
        customerSkuData(ProductVariables.SKU_SIMPLE),
        itrDataFrame(ProductVariables.SPECIAL_PRICE))

    val refSkus = CampaignUtils.generateReferenceSku(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return refSkus
  }

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???
  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, inDataFrame3: DataFrame): DataFrame = ???
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???
}
