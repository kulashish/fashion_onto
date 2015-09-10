package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 10/9/15.
 */
class ShortlistReminderCampaign {
  def runCampaign(fullShortlistData: DataFrame, recommendationsData: DataFrame, yesterdayItrData: DataFrame) = {

    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)

    val shortlist3rdDayData = CampaignInput.loadNthDayShortlistData(fullShortlistData, 3)

    val dfCustomerSelection = wishListCustomerSelector.customerSelection(shortlist3rdDayData)

    //TODO: Fix recommendation Data

    //TODO: generate reference skus
    //    val refSkus = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

  }

}
