package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.actions.ActionFactory
import com.jabong.dap.campaign.customerselect.{CustomerSelectionFactory, CustomerSelection}
import com.jabong.dap.campaign.recommendation.RecommenderFactory

/**
 * Created by jabong1145 on 7/7/15.
 */
object CampaignProducer {

  def getFactory(factoryType:String): CampaignFactory = {
    if (factoryType == null) {
      return null
    }
    if (factoryType.equalsIgnoreCase("CustomerSelection")) {
      return new CustomerSelectionFactory()
    }
    else if (factoryType.equalsIgnoreCase("Action")) {
      return new ActionFactory()
    }

    else if(factoryType.equalsIgnoreCase("Recommendation")) {
      return new RecommenderFactory()
    }

    return null
  }

}
