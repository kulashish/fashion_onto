package com.jabong.dap.campaign.actions

import com.jabong.dap.campaign.customerselect.CustomerSelection
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender

/**
 * Created by jabong1145 on 4/7/15.
 */
class ActionFactory extends CampaignFactory {
  override def getCustomer(customerType: String): CustomerSelection = ???

  override def getAction(actionType: String): Action = {
    if(actionType==null){
      return null
    }
    if(actionType.equalsIgnoreCase("CancelReTarget")){
      return new CancelReTarget()
    }
    return null
  }

  override def getRecommendation(recType: String): Recommender = ???
}
