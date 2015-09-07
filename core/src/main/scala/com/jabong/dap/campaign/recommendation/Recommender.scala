package com.jabong.dap.campaign.recommendation

import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashMap

/**
 * Created by rahul (base recommender interface) on 22/6/15.
 */
trait Recommender extends java.io.Serializable {

  // given [(customerId, refSkuList)] ---> [(customerId, refSkuList, recommendationsList)]
  // 8 recommendations
  def generateRecommendation(orderData: DataFrame, recommendations: DataFrame): DataFrame

}
