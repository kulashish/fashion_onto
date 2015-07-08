package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.actions.Action
import com.jabong.dap.campaign.customerselect.CustomerSelection
import com.jabong.dap.campaign.manager.CampaignFactory

/**
Recommender Factory
  */
class RecommenderFactory extends CampaignFactory {

  override def getCustomer(customerType: String): CustomerSelection = {
    null
  }

  override def getAction(action: String): Action = {
    null
  }

  override def getRecommendation(recType: String): Recommender = {
    if(recType == null) {
      return null
    }
    null
  }
}
