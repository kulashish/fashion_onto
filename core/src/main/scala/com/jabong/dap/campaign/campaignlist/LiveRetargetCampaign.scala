package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import org.apache.spark.sql.DataFrame

/**
 *  Live Retarget Campaign  Class
 */
class LiveRetargetCampaign {

  def runCampaign(customerOrderData: DataFrame, orderItemData: DataFrame, yesterdayItrData: DataFrame, brickMvpRecommendations:DataFrame): Unit = {

    // x = run retargeting campaign common customer selection
    val returnCancelCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.RETURN_CANCEL)

    // find customers with required order-item status during last day
    // FIXME: we will have to filter for last 30 days later for customerOrderData and last day for orderItemData
    val targetCustomersWithOrderItems = returnCancelCustomerSelector.customerSelection(customerOrderData, orderItemData)

    // FIXME: past campaign sent or not filter

    //run cancel retargeting campaign
    val cancelCampaign = new LiveCancelReTargetCampaign()
    cancelCampaign.runCampaign(targetCustomersWithOrderItems,yesterdayItrData,brickMvpRecommendations)

    // run return retargeting campaign
    val returnCampaign = new LiveReturnReTargetCampaign()
    returnCampaign.runCampaign(targetCustomersWithOrderItems,yesterdayItrData,brickMvpRecommendations)

  }
}
