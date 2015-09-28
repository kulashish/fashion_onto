package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
 * 3. pick n ref based on special price (descending)
 * 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU
 */
object LowStock extends Logging {

  // input will be [(id_customer, sku simple)] or [(id_customer, sku)]
  // case 1: only sku simple
  // case 2: only sku
  def skuFilter(customerSkuData: DataFrame, itrDataFrame: DataFrame): DataFrame = {
    if (customerSkuData == null || itrDataFrame == null) {
      return null
    }

    val filteredSku = customerSkuData.join(
      itrDataFrame, customerSkuData(ProductVariables.SKU_SIMPLE) === itrDataFrame(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .filter(ProductVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)
      .select(customerSkuData(CustomerVariables.FK_CUSTOMER),
        customerSkuData(ProductVariables.SKU_SIMPLE),
        itrDataFrame(ProductVariables.SPECIAL_PRICE),
        itrDataFrame(ProductVariables.BRAND),
        itrDataFrame(ProductVariables.BRICK),
        itrDataFrame(ProductVariables.MVP),
        itrDataFrame(ProductVariables.GENDER),
        itrDataFrame(ProductVariables.PRODUCT_NAME))

    //  val refSkus = CampaignUtils.generateReferenceSkus(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return filteredSku
  }

}
