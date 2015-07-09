package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.customerselection.CustomerSelector
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.common.constants.campaign.SkuSelection


class SkuSelectorFactory extends CampaignFactory {
  override def getCustomerSelector(customerType: String): CustomerSelector = ???

  override def getSkuSelector(actionType: String): SkuSelector = {
    if(actionType==null){
      return null
    }
    if(actionType.equalsIgnoreCase(SkuSelection.CANCEL_RETARGET)){
      return new CancelReTarget()
    }
    return null
  }

  override def getRecommender(recType: String): Recommender = ???
}
