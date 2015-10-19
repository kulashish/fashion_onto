package com.jabong.dap.campaign.recommendation

import org.apache.spark.sql.DataFrame

class NullRecommender extends Recommender {
  // given [(customerId, refSkuList)] ---> [(customerId, refSkuList, recommendationsList)]

  override def generateRecommendation(refSkus: DataFrame, recommendation: DataFrame, rectType: String): DataFrame = {
    return null
  }

}
