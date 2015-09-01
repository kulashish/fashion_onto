package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.{ NullInputException, WrongInputException }
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 27/8/15.
 */
object PivotRecommendation extends CommonRecommendation with Serializable {

  @throws(classOf[IllegalArgumentException])
  @throws(classOf[NullInputException])
  @throws(classOf[WrongInputException])
  def generateRecommendation(orderItemFullData: DataFrame, yesterdayItrData: DataFrame, pivotKey: String, numRecs: Int, incrDate: String) {
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
    //println("top products count:-" + topProducts.count)

    val orderItem7DaysWithWeeklySale = createWeeklyAverageSales(last7DaysOrderItemData)

    val weeklySaleData = addWeeklyAverageSales(orderItem7DaysWithWeeklySale, topProducts)

    val completeSkuData = skuCompleteData(weeklySaleData, yesterdayItrData)

    val skuDataAfterInventoryFilter = inventoryCheck(completeSkuData)
    //println("sku complete data:-" + skuDataAfterInventoryFilter.count + "\t" + skuDataAfterInventoryFilter.printSchema() + "\n")
    val pivotKeyArray = RecommendationUtils.getPivotArray(pivotKey)(0)
    val recommendedSkus = genRecommend(skuDataAfterInventoryFilter, pivotKeyArray, Schema.recommendationOutput, numRecs)
    //println("rec skus data:-" + recommendedSkus.count + "\t Values :-" + recommendedSkus.show(100))

    RecommendationOutput.writeRecommendation(recommendedSkus)
  }
}
