package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.{ NullInputException, WrongInputException, OptionUtils }
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.acq.common.ParamInfo
import grizzled.slf4j.Logging

/**
 * Created by rahul on 25/8/15.
 */
object RecommendationGenerator extends Logging {
  /**
   * start the recommendation generation Process
   * @param paramInfo
   */
  @throws(classOf[WrongInputException])
  @throws(classOf[NullInputException])
  def start(paramInfo: ParamInfo) {
    val incrDate = OptionUtils.getOptValue(paramInfo.incrDate, TimeUtils.YESTERDAY_FOLDER)
    val pivotKey = OptionUtils.getOptValue(paramInfo.subType, Recommendation.BRICK_MVP_SUB_TYPE)
    val numberDays = Recommendation.ORDER_ITEM_DAYS

    logger.info("Recommendation Process has started for pivotkey:-" + pivotKey + " date::-" + incrDate)
    RecommendationInput.loadCommonDataSets(incrDate)
    PivotRecommendation.generateRecommendation(RecommendationInput.orderItemFullData, RecommendationInput.lastdayItrData, pivotKey, Recommendation.NUM_RECOMMENDATIONS, incrDate, numberDays)
    // Added for search recommendation
    PivotRecommendation.generateRecommendation(RecommendationInput.orderItemFullData, RecommendationInput.lastdayItrData, Recommendation.BRICK_MVP_SEARCH_SUB_TYPE, Recommendation.SEARCH_NUM_RECOMMENDATIONS, incrDate, Recommendation.SEARCH_RECOMMENDATION_ORDER_ITEM_DAYS)

    PivotAddressRecommendation.generateRecommendation(RecommendationInput.orderItemFullData, RecommendationInput.lastdayItrData, Recommendation.BRAND_MVP_CITY_STATE, Recommendation.NUM_RECOMMENDATIONS, incrDate, numberDays)
    logger.info("Recommendation successfully generated for pivotkey:-" + pivotKey + " date::-" + incrDate)
  }

}
