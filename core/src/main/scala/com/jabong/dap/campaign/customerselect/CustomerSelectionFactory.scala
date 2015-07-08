package com.jabong.dap.campaign.customerselect

import com.jabong.dap.campaign.actions.Action
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender

/**
 * Created by jabong1145 on 6/7/15.
 */
class CustomerSelectionFactory extends CampaignFactory{

  override def getCustomer(customerType: String): CustomerSelection = {
    if(customerType==null){
      return null
    }

    if(customerType.equalsIgnoreCase("ReturnCancel")){
      return new ReturnCancel()
    }

    return null
  }

  override def getAction(action: String): Action = ???

  override def getRecommendation(recType: String): Recommender = ???

}
