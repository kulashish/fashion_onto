package com.jabong.dap.campaign.recommendation

import org.apache.spark.sql.DataFrame

class NullRecommender extends Recommender {
  // given [(customerId, refSkuList)] ---> [(customerId, refSkuList, recommendationsList)]
  override def recommend(refSkus: DataFrame): DataFrame = {
    return refSkus
  }

  override def generateRecommendation(orderData: DataFrame): DataFrame = {
    return null
  }


}
