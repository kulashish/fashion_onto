package com.jabong.dap.campaign.recommendation.generator

import java.sql.Struct

import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.collection.immutable.HashMap

/**
 * Created by rahul aneja on 27/8/15.
 */
object RecommendationUtils extends Serializable {

  var recommendationType = new HashMap[String, Array[(Array[String], StructType, String)]]
  recommendationType += (

    Recommendation.BRICK_MVP_SUB_TYPE -> Array((Recommendation.BRICK_MVP_PIVOT, Schema.brickMvpRecommendationOutput, DataSets.BRICK_MVP_RECOMMENDATIONS)),
    Recommendation.BRICK_MVP_SEARCH_SUB_TYPE -> Array((Recommendation.BRICK_MVP_PIVOT, Schema.brickMvpRecommendationOutput, DataSets.BRICK_MVP_SEARCH_RECOMMENDATIONS)),
    Recommendation.BRAND_MVP_SUB_TYPE -> Array((Recommendation.BRAND_MVP_PIVOT, Schema.brandMvpRecommendationOutput, DataSets.BRICK_MVP_RECOMMENDATIONS)),
    Recommendation.BRICK_PRICE_BAND_SUB_TYPE -> Array((Recommendation.BRICK_PRICE_BAND_PIVOT, Schema.brickPriceBandRecommendationOutput, DataSets.BRICK_PRICE_BAND_RECOMMENDATIONS)),
    Recommendation.MVP_COLOR_SUB_TYPE -> Array((Recommendation.MVP_COLOR_PIVOT, Schema.mvpColorRecommendationOutput, DataSets.MVP_COLOR_RECOMMENDATIONS)),
    Recommendation.MVP_DISCOUNT_SUB_TYPE -> Array((Recommendation.MVP_DISCOUNT_PIVOT, Schema.mvpDiscountRecommendationOutput, DataSets.MVP_DISCOUNT_RECOMMENDATIONS)),
    Recommendation.BRAND_MVP_CITY_SUB_TYPE -> Array((Recommendation.BRAND_MVP_CITY_PIVOT, Schema.brandMvpCityRecommendationOutput, DataSets.BRAND_MVP_CITY_RECOMMENDATIONS)),
    Recommendation.BRAND_MVP_CITY_STATE -> Array((Recommendation.BRAND_MVP_CITY_PIVOT, Schema.brandMvpCityRecommendationOutput, DataSets.BRAND_MVP_CITY_RECOMMENDATIONS),
      (Recommendation.BRAND_MVP_STATE_PIVOT, Schema.brandMvpStateRecommendationOutput, DataSets.BRAND_MVP_STATE_RECOMMENDATIONS)),

      Recommendation.ALL -> Array((Recommendation.BRICK_MVP_PIVOT, Schema.brickMvpRecommendationOutput, DataSets.BRICK_MVP_RECOMMENDATIONS),
        (Recommendation.BRAND_MVP_PIVOT, Schema.brandMvpRecommendationOutput, DataSets.BRAND_MVP_RECOMMENDATIONS),
        (Recommendation.BRICK_PRICE_BAND_PIVOT, Schema.brickPriceBandRecommendationOutput, DataSets.BRICK_PRICE_BAND_RECOMMENDATIONS),
        (Recommendation.MVP_COLOR_PIVOT, Schema.mvpColorRecommendationOutput, DataSets.MVP_COLOR_RECOMMENDATIONS),
        (Recommendation.MVP_DISCOUNT_PIVOT, Schema.mvpDiscountRecommendationOutput, DataSets.MVP_DISCOUNT_RECOMMENDATIONS))
  )

  var RecommendationGenderMap = new HashMap[String, String]
  RecommendationGenderMap += (
    "WOMEN" -> "WOMEN!UNISEX",
    "MEN" -> "MEN!UNISEX",
    "GIRLS" -> "GIRLS!BOYS,GIRLS",
    "UNISEX" -> "UNISEX!MEN!WOMEN",
    "MEN,WOMEN" -> "UNISEX!MEN,WOMEN",
    "BOYS,GIRLS" -> "BOYS!GIRLS!BOYS,GIRLS",
    "BOYS" -> "BOYS!BOYS,GIRLS",
    "INFANTS" -> "INFANTS",
    "BLANK" -> "BLANK"
  )

  var DesiredInventoryLevel = new HashMap[String, Int]
  DesiredInventoryLevel += (
    "SUNGLASSES" -> 2,
    "WOMEN FOOTWEAR" -> 3,
    "KIDS APPAREL" -> 2,
    "WATCHES" -> 3,
    "BEAUTY" -> 3,
    "FURNITURE" -> 2,
    "SPORTS EQUIPMENT" -> 2,
    "WOMEN APPAREL" -> 2,
    "HOME" -> 2,
    "MEN FOOTWEAR" -> 3,
    "MEN APPAREL" -> 2,
    "JEWELLERY" -> 2,
    "FRAGRANCE" -> 2,
    "KIDS FOOTWEAR" -> 3,
    "BAGS" -> 2,
    "TOYS" -> 2
  )

  def getPivotArray(pivotKey: String): Array[(Array[String], StructType, String)] = {
    if (pivotKey == null) {
      return null
    }
    return recommendationType.getOrElse(pivotKey, null)
  }
  /**
   * get recommended  gender
   * @param gender
   * @return
   */
  def getRecommendationGender(gender: Any): String = {
    if (gender == null) {
      return null
    }
    return RecommendationGenderMap.getOrElse(gender.toString, "BLANK")
  }

  /**
   * Inventory Filter
   * @param category
   * @param stock
   * @param weeklyAverage
   * @return
   */
  def inventoryFilter(category: String, numberSkuSimples: Long, stock: Long, weeklyAverage: java.lang.Double): Boolean = {
    //    if (category == null || stock == 0 || weeklyAverage == 0 || stock < weeklyAverage) {
    //      return false
    //    }
    if (weeklyAverage == 0 || weeklyAverage == null) {
      if (category == null) {
        return false
      }
      val stockMultiplier = DesiredInventoryLevel.getOrElse(category, null)
      if (stockMultiplier == null) {
        return false
      }
      return stock >= numberSkuSimples * stockMultiplier.asInstanceOf[Int]
    } else {
      return stock >= 2 * weeklyAverage
    }
  }

  /**
   *
   * @param data
   * @param days
   * @param Type
   * @param column
   * @return
   */
  def daysData(data: DataFrame, days: Int, Type: String, column: String): DataFrame = {
    var expType: String = ""
    if (Type == "last") {
      expType = "<"
    } else
      expType = ">"

    if (data == null || days < 0 || column == null) {
      return null
    }
    //FIXME:change unix time stamp to last day function in campaign utils
    val lastDaysData = data.filter("(unix_timestamp() -unix_timestamp(" + column + ",'yyyy-MM-dd HH:mm:ss.S'))/60/60/24<" + days)
    return lastDaysData
  }

}
