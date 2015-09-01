package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.WrongInputException
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 27/8/15.
 */
object PivotRecommendation extends CommonRecommendation {

  def generateRecommendation(orderItemFullData: DataFrame, yesterdayItrData: DataFrame, pivotKey:String , numRecs: Int , incrDate: String) {
    if(RecommendationUtils.getPivotArray(pivotKey) == null){
      logger.info(("Invalid pivotKey:- %d",pivotKey))
      throw new WrongInputException(("Invalid pivotKey:"+pivotKey))
    }
    val last30DaysOrderItemData = RecommendationInput.lastNdaysData(orderItemFullData,Recommendation.ORDER_ITEM_DAYS,incrDate)

    val last7DaysOrderItemData = RecommendationInput.lastNdaysData(orderItemFullData,7,incrDate)

    val topProducts = topProductsSold(last30DaysOrderItemData)
    println("top products count:-" + topProducts.count)

    val skuData = skuCompleteData(topProducts, yesterdayItrData)
    println("sku complete data:-" + skuData.count)
    val pivotKeyArray = RecommendationUtils.getPivotArray(pivotKey)(0)
    val recommendedSkus = genRecommend(skuData, pivotKeyArray, Schema.recommendationOutput,numRecs)
    println("rec skus data:-" + recommendedSkus.count + "\t Values :-" + recommendedSkus.show(100))
    RecommendationOutput.writeRecommendation(recommendedSkus)
  }
}
