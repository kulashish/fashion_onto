package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

class WishlistLowStockCampaign {
  def runCampaign(customerSelected: DataFrame,itrSku30DaysData:DataFrame,itrSkuSimpleYesterdayData:DataFrame): Unit = {
    // select customers who have added one or more items to wishlist during 30 days

    // sku filter
    // 1. order should not have been placed for the ref sku yet
    // 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
    // 3. pick n ref based on special price (descending)
    // 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU

    // null recommendation

    val lowStockSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).
      getSkuSelector(SkuSelection.SKU_LOW_STOCK)

    val refSkus = lowStockSkuSelector.skuFilter(customerSelected,itrSku30DaysData,itrSkuSimpleYesterdayData)

  val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN)
  //save campaign Output
  CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
    + CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))
}
