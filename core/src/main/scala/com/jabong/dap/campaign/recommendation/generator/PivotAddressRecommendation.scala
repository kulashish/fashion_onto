package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.common.constants.variables.{ SalesAddressVariables, SalesOrderItemVariables, SalesOrderVariables }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 18/11/15.
 */
object PivotAddressRecommendation extends CommonRecommendation with Serializable {
  /**
   * Abstract method to generate recommendations
   * @param orderItemFullData
   * @param yesterdayItrData
   * @param pivotKey
   * @param numRecs
   * @param incrDate
   * @return
   */
  override def generateRecommendation(orderItemFullData: DataFrame, yesterdayItrData: DataFrame, pivotKey: String, numRecs: Int, incrDate: String, numDays: Int) = {

    val salesAddressFullData = RecommendationInput.salesAddressFullData
    val salesOrder30DaysData = RecommendationInput.salesOrder30DaysData

    val last30DaysOrderItemData = RecommendationInput.lastNdaysData(orderItemFullData, numDays, incrDate)

    val salesAddressFullDataWithState = addStateFromMapping(salesAddressFullData, RecommendationInput.cityZoneMapping)

    val salesJoinedData = salesOrder30DaysData.join(last30DaysOrderItemData, salesOrder30DaysData(SalesOrderVariables.ID_SALES_ORDER) ===
      last30DaysOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER).join(salesAddressFullDataWithState, salesOrder30DaysData(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING)
      === salesAddressFullDataWithState(SalesAddressVariables.ID_SALES_ORDER_ADDRESS), SQL.INNER)
      .select(salesAddressFullDataWithState(SalesAddressVariables.CITY),
        salesAddressFullDataWithState(Recommendation.RECOMMENDATION_STATE),
        last30DaysOrderItemData(SalesOrderItemVariables.SKU),
        last30DaysOrderItemData(SalesOrderItemVariables.CREATED_AT),
        last30DaysOrderItemData(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS))

    val last7DaysOrderItemDataWithCityState = RecommendationInput.lastNdaysData(salesJoinedData, 7, incrDate)

    val varProductsWithCountSold = productsWithCountSold(salesJoinedData, true)

    // create list of (sku, avg count sold in last 7 days) : only for skus sold in last 7 days
    val orderItem7DaysWithWeeklySale = createWeeklyAverageSales(last7DaysOrderItemDataWithCityState, true)

    // merge to product (sku, count sold in last 30 days, avg count sold in last 7 days or NULL)
    val weeklySaleData = addWeeklyAverageSales(orderItem7DaysWithWeeklySale, varProductsWithCountSold, true)

    // Join with itr data to get field like mvp , price band ,category , gender etc
    val completeSkuData = skuCompleteData(weeklySaleData, yesterdayItrData)

    // Filter skus which has less stock than desired inventory level
    val skuDataAfterInventoryFilter = inventoryCheck(completeSkuData).cache()

    val pivotArray = RecommendationUtils.getPivotArray(pivotKey)
    for (pivot <- pivotArray) {
      logger.info("Recommendation generation for" + pivotKey + "started")
      val pivotKeyArray = pivot._1
      val pivotBasedOutputSchema = pivot._2
      val recommedationType = pivot._3
      // function which generates recommendations
      val recommendedSkus = genRecommend(skuDataAfterInventoryFilter, pivotKeyArray, pivotBasedOutputSchema, numRecs)
      RecommendationOutput.writeRecommendation(recommendedSkus, recommedationType)
      logger.info("Recommendation generation for" + pivotKey + "ended")
    }

  }
}
