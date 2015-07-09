package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.campaignlist.LiveRetargetCampaign

/**
 *  Campaign Manager will run multiple campaign based On Priority
 *  TODO: this class will need to be refactored to create a proper data flow of campaigns
 *
 */
class CampaignManager {

  def execute() = {

    val liveRetargetCampaign = new LiveRetargetCampaign()
    liveRetargetCampaign.runCampaign(null)

  }

}
