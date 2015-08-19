package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.customerselection.{ CustomerSelectorFactory, CustomerSelector }
import com.jabong.dap.campaign.recommendation.RecommenderFactory
import com.jabong.dap.common.constants.campaign.CampaignCommon

/**
 * Created by rahul for campaign production on 7/7/15.
 */
object CampaignProducer {

  def getFactory(factoryType: String): CampaignFactory = {
    if (factoryType == null) {
      return null
    }
    if (factoryType.equalsIgnoreCase(CampaignCommon.CUSTOMER_SELECTOR)) {
      return new CustomerSelectorFactory()
    } else if (factoryType.equalsIgnoreCase(CampaignCommon.RECOMMENDER)) {
      return new RecommenderFactory()
    }

    return null
  }

}
