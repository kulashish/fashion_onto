package com.jabong.dap.campaign.recommendation

import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashMap

/**
 * Created by rahul (base recommender interface) on 22/6/15.
 */
trait Recommender extends java.io.Serializable {

  // given [(customerId, refSkuList)] ---> [(customerId, refSkuList, recommendationsList)]
  // 8 recommendations
  def generateRecommendation(orderData: DataFrame, yesterdayItr: DataFrame): DataFrame

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
    "WOMEN_FOTWEAR" -> 3,
    "KIDS_APAREL" -> 2,
    "WATCHES" -> 3,
    "BEAUTY" -> 3,
    "FURNITURE" -> 2,
    "SPORTS_EQUIPMENT" -> 2,
    "WOMEN_APAREL" -> 2,
    "HOME" -> 2,
    "MEN_FOTWEAR" -> 3,
    "MEN_APAREL" -> 2,
    "JEWELLERY" -> 2,
    "FRAGRANCE" -> 2,
    "KIDS_FOTWEAR" -> 3,
    "BAGS" -> 2,
    "TOYS" -> 2
  )


  def getRecommendationGender(gender: Any): String = {
    if (gender == null) {
      return null
    }
    return RecommendationGenderMap.getOrElse(gender.toString, "BLANK")
  }

  def inventoryWeekNotSold(category: String, stock: Int, weeklyAverage: Int): Boolean = {
    if (category == null || stock == 0 || weeklyAverage == 0 || stock < weeklyAverage) {
      return false
    }
    val stockMultiplier = DesiredInventoryLevel.getOrElse(category, null)
    if (stockMultiplier == null) {
      return false
    }
    return stock >= stockMultiplier.asInstanceOf[Int] * weeklyAverage
  }

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
