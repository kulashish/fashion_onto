package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.customerselection.CustomerSelector
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender

/**
 * Created by jabong1145 on 4/7/15.
 */
class SkuSelectorFactory extends CampaignFactory {
  override def getCustomerSelector(customerType: String): CustomerSelector = ???

  override def getSkuSelector(actionType: String): SkuSelector = {
    if(actionType==null){
      return null
    }
    if(actionType.equalsIgnoreCase("CancelReTarget")){
      return new CancelReTarget()
    }
    return null
  }

  override def getRecommender(recType: String): Recommender = ???
}
