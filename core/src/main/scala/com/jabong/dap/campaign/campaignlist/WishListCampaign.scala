package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 27/7/15.
 */
class WishListCampaign {

  def runCampaign(shortListFullData: DataFrame, itrSku30DaysData: DataFrame, itrSkuSimpleYesterdayData: DataFrame): Unit = {
    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)
    val customerSelected = wishListCustomerSelector.customerSelection(shortListFullData, 30)

    // call iod campaign
    val wishListIODCampaign = new WishlistIODCampaign()
    wishListIODCampaign.runCampaign(customerSelected, itrSku30DaysData, itrSkuSimpleYesterdayData)

    // call lowstock campaign
    val wishListLowStockCampaign = new WishlistLowStockCampaign()
    wishListLowStockCampaign.runCampaign(customerSelected, itrSku30DaysData, itrSkuSimpleYesterdayData)
  }
}
