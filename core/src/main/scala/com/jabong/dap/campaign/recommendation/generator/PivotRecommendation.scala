package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.{ NullInputException, WrongInputException }
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 27/8/15.
 */
object PivotRecommendation extends CommonRecommendation with Serializable {

  /**
   *  Actual method which generates recommendations based on pivotKey e.g brick mvp , brand mvp etc
   * @param orderItemFullData
   * @param yesterdayItrData
   * @param pivotKey
   * @param numRecs
   * @param incrDate
   */
  @throws(classOf[IllegalArgumentException])
  @throws(classOf[NullInputException])
  @throws(classOf[WrongInputException])
  override def generateRecommendation(orderItemFullData: DataFrame, yesterdayItrData: DataFrame, pivotKey: String, numRecs: Int, incrDate: String) {
    require(orderItemFullData != null, "order item full data cannot be null ")
    require(yesterdayItrData != null, "yesterdayItrData  cannot be null ")
    require(pivotKey != null, "pivotKey cannot be null ")
    require(numRecs != 0, "numRecs cannot be zero ")
    require(incrDate != null, "incrDate cannot be null ")

    if (RecommendationUtils.getPivotArray(pivotKey) == null) {
      logger.info(("Invalid pivotKey:- %d", pivotKey))
      throw new WrongInputException(("Invalid pivotKey:" + pivotKey))
    }
    val last30DaysOrderItemData = RecommendationInput.lastNdaysData(orderItemFullData, Recommendation.ORDER_ITEM_DAYS, incrDate)

    val last7DaysOrderItemData = RecommendationInput.lastNdaysData(orderItemFullData, 7, incrDate)

    val topProducts = topProductsSold(last30DaysOrderItemData)

    val orderItem7DaysWithWeeklySale = createWeeklyAverageSales(last7DaysOrderItemData)

    val weeklySaleData = addWeeklyAverageSales(orderItem7DaysWithWeeklySale, topProducts)

    // Join with itr data to get field like mvp , price band ,category , gender etc
    val completeSkuData = skuCompleteData(weeklySaleData, yesterdayItrData)

    // Filter skus which has less stock than desired inventory level
    val skuDataAfterInventoryFilter = inventoryCheck(completeSkuData)

    val pivotKeyArray = RecommendationUtils.getPivotArray(pivotKey)(0)
    // function which generates recommendations
    //FIXME: change fixed brickMvpRecommendationOutput  to mapping based
    val recommendedSkus = genRecommend(skuDataAfterInventoryFilter, pivotKeyArray, Schema.brickMvpRecommendationOutput, numRecs)

    RecommendationOutput.writeRecommendation(recommendedSkus)
  }
}
