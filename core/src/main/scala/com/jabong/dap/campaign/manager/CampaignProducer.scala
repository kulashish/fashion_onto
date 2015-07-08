package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.skuselection.SkuSelectorFactory
import com.jabong.dap.campaign.customerselection.{CustomerSelectorFactory, CustomerSelector}
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
      return new CustomerSelectorFactory()
    }
    else if (factoryType.equalsIgnoreCase("SkuSelection")) {
      return new SkuSelectorFactory()
    }

    else if(factoryType.equalsIgnoreCase("Recommendation")) {
      return new RecommenderFactory()
    }

    return null
  }

}
