package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{SkuSelection, CustomerSelection, CampaignCommon}
import org.apache.spark.sql.DataFrame

class WishlistFollowupCampaign {
  def runCampaign(shortListFullData: DataFrame,itrSku30DaysData:DataFrame,itrSkuSimpleYesterdayData:DataFrame): Unit = {

    // select customers who have added one or more items to wishlist during last day

    // sku filter 
    // 1. order should not have been placed for the ref sku yet
    // 2. pick based on special price (descending)

    // null recommendation
    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)
    val customerSelected =  wishListCustomerSelector.customerSelection(shortListFullData,1)

    val followUpSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).
      getSkuSelector(SkuSelection.FOLLOW_UP)

    val refSkus = followUpSkuSelector.skuFilter(customerSelected,itrSku30DaysData,itrSkuSimpleYesterdayData)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN)
    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))



  }
}