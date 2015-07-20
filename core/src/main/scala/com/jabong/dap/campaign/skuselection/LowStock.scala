package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{CustomerVariables, ProductVariables}
import org.apache.spark.sql.DataFrame

/**
 * 1. order should not have been placed for the ref sku yet
 * 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
 * 3. pick n ref based on special price (descending)
 * 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU
 */
class LowStock extends SkuSelector {

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  // input will be [(id_customer, sku simple)] or [(id_customer, sku)]
  // case 1: only sku simple
  // case 2: only sku
  override def skuFilter(customerSkuData: DataFrame, itrDataFrame: DataFrame, campaignName: String): DataFrame = {
    if(customerSkuData == null || itrDataFrame==null || campaignName==null){
      return null
    }
    var filteredSku:DataFrame = null
    if(campaignName==CampaignCommon.INVALID_CAMPAIGN){
     filteredSku=  customerSkuData.join(itrDataFrame,customerSkuData(ProductVariables.SKU)===itrDataFrame(ProductVariables.SKU),"inner")
        .filter(itrDataFrame(ProductVariables.STOCK+" <= "+CampaignCommon.LOW_STOCK_VALUE))
        .select(customerSkuData(CustomerVariables.FK_CUSTOMER),customerSkuData(ProductVariables.SKU),customerSkuData(ProductVariables.SPECIAL_PRICE))
    } else if(campaignName==CampaignCommon.WISHLIST_CAMPAIGN) {
        // separate sku and sku simples


    }

      return filteredSku
  }

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???
}
