package com.jabong.dap.campaign.campaign_list

import com.jabong.dap.campaign.manager.CampaignProducer

/**
 * Created by jabong1145 on 7/7/15.
 */
class LiveCancelReTargetCampaign {

  def runCampaign(): Unit ={

    val campaignFactory = CampaignProducer.getFactory("CustomerSelection")

    val returnCancelCustomer = campaignFactory.getCustomerSelector("ReturnCancel")

//    returnCancelCustomer.customerSelection()

  }

}
