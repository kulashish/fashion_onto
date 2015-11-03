package com.jabong.dap.model.customer.campaigndata

/**
 * Created by mubarak on 30/10/15.
 */

/**
 * SUNGLASSES_COUNT
 * WOMEN_FOOTWEAR_COUNT
 * KIDS_APPAREL_COUNT
 * WATCHES_COUNT
 * BEAUTY_COUNT
 * FURNITURE_COUNT
 * SPORTS_EQUIPMENT_COUNT
 * WOMEN_APPAREL_COUNT
 * HOME_COUNT
 * MEN_FOOTWEAR_COUNT
 * MEN_APPAREL_COUNT
 * JEWELLERY_COUNT
 * FRAGRANCE_COUNT
 * KIDS_FOOTWEAR_COUNT
 * BAGS_COUNT
 * TOYS_COUNT
 */

/**
 * SUNGLASSES_AVG_ITEM_PRICE
 * WOMEN_FOOTWEAR_AVG_ITEM_PRICE
 * KIDS_APPAREL_AVG_ITEM_PRICE
 * WATCHES_AVG_ITEM_PRICE
 * BEAUTY_AVG_ITEM_PRICE
 * FURNITURE_AVG_ITEM_PRICE
 * SPORT_EQUIPMENT_AVG_ITEM_PRICE
 * JEWELLERY_AVG_ITEM_PRICE
 * WOMEN_APPAREL_AVG_ITEM_PRICE
 * HOME_AVG_ITEM_PRICE
 * MEN_FOOTWEAR_AVG_ITEM_PRICE
 * MEN_APPAREL_AVG_ITEM_PRICE
 * FRAGRANCE_AVG_ITEM_PRICE
 * KIDS_FOOTWEAR_AVG_ITEM_PRICE
 * TOYS_AVG_ITEM_PRICE
 * BAGS_AVG_ITEM_PRICE
 */
object CustCatPurchase {

  val catagories: List[String] = List("SUNGLASSES",
    "WOMEN_FOOTWEAR",
    "KIDS_APPAREL",
    "WATCHES",
    "BEAUTY",
    "FURNITURE",
    "SPORT_EQUIPMENT",
    "JEWELLERY",
    "WOMEN_APPAREL",
    "HOME",
    "MEN_FOOTWEAR",
    "MEN_APPAREL",
    "FRAGRANCE",
    "KIDS",
    "TOYS",
    "BAGS")

  def getCatCount(map: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]): List[(Int, Double)] = {
    var list = scala.collection.mutable.ListBuffer[(Int, Double)]()
    catagories.foreach{
      e =>
        var count = 0
        var sum = 0.0
        if (map.contains(e)) {
          val m = map(e)
          m.foreach{
            e =>
              count = e._1
              sum = e._2
          }
          val ele = Tuple2(count, (sum / count))
          list.+=(ele)
        } else {
          val ele = Tuple2(count, sum)
          list.+=(ele)
        }
    }
    list.toList
  }

}
