package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.campaign.Recommendation
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashMap

/**
 * Created by rahul (base recommender interface) on 22/6/15.
 */
trait Recommender extends java.io.Serializable {

  // given [(customerId, refSkuList)] ---> [(customerId, refSkuList, recommendationsList)]
  def generateRecommendation(refSkus: DataFrame, recommendations: DataFrame, recType: String = null, numRecSkus:Int = 8): DataFrame

}
