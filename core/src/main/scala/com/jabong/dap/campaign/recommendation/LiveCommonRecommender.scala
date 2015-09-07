package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignMergedFields }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables }
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.types._

/**
 * Created by rahul aneja on 21/8/15.
 */
class LiveCommonRecommender extends Recommender with Logging {
  /**
   * Place holder function which will get recommended skus
   * @param refSkus
   * @param recommendations
   * @return
   */
  override def generateRecommendation(refSkus: DataFrame, recommendations: DataFrame): DataFrame = {
    require(refSkus != null, "refSkus cannot be null")
    require(recommendations != null, "recommendations cannot be null")

    val recommendationJoined = refSkus.join(recommendations, refSkus(ProductVariables.BRICK) === recommendations(ProductVariables.BRICK)
      && refSkus(ProductVariables.MVP) === recommendations(ProductVariables.MVP)
      && refSkus(ProductVariables.GENDER) === recommendations(ProductVariables.GENDER))
      .select(refSkus(CampaignMergedFields.REF_SKU1),
        refSkus(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        refSkus(CustomerVariables.FK_CUSTOMER),
        recommendations(ProductVariables.BRICK),
        recommendations(ProductVariables.MVP),
        recommendations(ProductVariables.GENDER),
        recommendations(ProductVariables.RECOMMENDATIONS))

    //  val refSkusWithRecommendations = recommendationJoined.map(row=> getRecommendedSkus(row(row.fieldIndex(CampaignMergedFields.REF_SKU1))
    //  ,row(row.fieldIndex(ProductVariables.RECOMMENDATIONS))))

    return recommendationJoined
  }

  /**
   *
   * @param refSku
   * @param recommendation
   * @return
   */
  def getRecommendedSkus(refSku: String, recommendation: List[(Long, String)]): List[String] = {
    require(refSku != null, "refSkus cannot be null")
    require(recommendation != null, "recommendation cannot be null")
    val outputSkus = recommendation.filterNot(_._2 == refSku).take(8).map(_._2)
    return outputSkus
  }
}
