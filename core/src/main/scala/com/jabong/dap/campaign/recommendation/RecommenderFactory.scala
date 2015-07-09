package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.skuselection.SkuSelector
import com.jabong.dap.campaign.customerselection.CustomerSelector
import com.jabong.dap.campaign.manager.CampaignFactory

/**
 * Recommender Factory
 */
class RecommenderFactory extends CampaignFactory {

  override def getCustomerSelector(customerType: String): CustomerSelector = {
    null
  }

  override def getSkuSelector(action: String): SkuSelector = {
    null
  }

  override def getRecommender(recType: String): Recommender = {
    if (recType == null) {
      return null
    } else if (recType.equalsIgnoreCase("Null")) {
      return new NullRecommender()
    }
    null
  }
}
