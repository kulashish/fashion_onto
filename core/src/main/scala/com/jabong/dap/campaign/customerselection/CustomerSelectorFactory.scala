package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.skuselection.SkuSelector
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.common.constants.campaign.CustomerSelection

/**
 * Created by jabong1145 on 6/7/15.
 */
class CustomerSelectorFactory extends CampaignFactory{

  override def getCustomerSelector(customerSelectionType: String): CustomerSelector = {
    if(customerSelectionType==null){
      return null
    }

    if(customerSelectionType.equalsIgnoreCase(CustomerSelection.RETURN_CANCEL)){
      return new ReturnCancel()
    }

    return null
  }

  override def getSkuSelector(action: String): SkuSelector = ???

  override def getRecommender(recType: String): Recommender = ???

}
