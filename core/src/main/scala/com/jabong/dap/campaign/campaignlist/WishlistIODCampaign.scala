package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

class WishlistIODCampaign {

  def runCampaign(customerSelected: DataFrame, itrSku30DaysData: DataFrame, itrSkuSimpleYesterdayData: DataFrame): Unit = {

    // select customers who have added one or more items to wishlist during 30 days

    // sku filter
    // 1. order should not have been placed for the ref sku yet
    // 2. Today's Special Price of SKU (SIMPLE – include size) is less than
    //      previous Special Price of SKU (when it was added to wishlist)
    // 3. This campaign shouldn’t have gone to the customer in the past 30 days for the same Ref SKU
    // 4. pick based on special price (descending)

    // null recommendation

    val iodSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).
      getSkuSelector(SkuSelection.SKU_ITEM_ON_DISCOUNT)

    val refSkus = iodSkuSelector.skuFilter(customerSelected, itrSku30DaysData, itrSkuSimpleYesterdayData)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_IOD_CAMPAIGN)
    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.WISHLIST_IOD_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))
  }

}
