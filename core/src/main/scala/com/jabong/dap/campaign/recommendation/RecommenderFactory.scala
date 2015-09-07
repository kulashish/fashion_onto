package com.jabong.dap.campaign.recommendation

import com.jabong.dap.campaign.customerselection.CustomerSelector
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.common.constants.campaign.Recommendation

/**
 * Recommender Factory
 */
class RecommenderFactory extends CampaignFactory {

  override def getCustomerSelector(customerType: String): CustomerSelector = {
    null
  }

  override def getRecommender(recType: String): Recommender = {
    if (recType == null) {
      return null
    } else if (recType.equalsIgnoreCase(Recommendation.LIVE_COMMON_RECOMMENDER)) {
      return new LiveCommonRecommender()
    } else if (recType.equalsIgnoreCase("Null")) {
      return new NullRecommender()
    }
    null
  }
}
