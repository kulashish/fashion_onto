package com.jabong.dap.common.constants.campaign

import com.jabong.dap.common.constants.variables.{ SalesAddressVariables, ProductVariables }

/**
 * Created by rahul for recommendation constants  on 9/7/15.
 */
object Recommendation {

  val LIVE_COMMON_RECOMMENDER = "live_common_recommender"
  val CALENDER_COMMON_RECOMMENDER = "calender_common_recommender"
  val SALES_ORDER_ITEM_SKU = "sales_order_item_sku"
  val NUMBER_LAST_30_DAYS_ORDERED = "number_last_30_days_ordered"
  val LAST_SOLD_DATE = "last_sold_date"

  val NUM_RECOMMENDATIONS = 20

  val SEARCH_NUM_RECOMMENDATIONS = 100

  val BRICK_MVP_SUB_TYPE = "brick_mvp"

  val BRICK_MVP_SEARCH_SUB_TYPE = "brick_mvp_search"


  val BRAND_MVP_SUB_TYPE = "brand_mvp"

  val BRAND_MVP_CITY_STATE = "brand_mvp_city_state"

  val BRAND_MVP_CITY_SUB_TYPE = "brand_mvp_city"
  val BRAND_MVP_STATE_SUB_TYPE = "brand_mvp_state"

  val MVP_DISCOUNT = "mvp_discount"
  val ALL = "all"
  val MVP_DISCOUNT_SUB_TYPE = "mvp_discount"

  val MVP_COLOR_SUB_TYPE = "mvp_color"

  val BRICK_PRICE_BAND_SUB_TYPE = "brick_price_band"

  val INVENTORY_FILTER = "inventory_filter"

  val WEEKLY_AVERAGE_SALE = "weekly_average_sale"

  val DISCOUNT_STATUS = "discount_status"

  val RECOMMENDATION_STATE = "STATE"

  val BRICK_MVP_PIVOT = Array(ProductVariables.BRICK, ProductVariables.MVP)
  val BRAND_MVP_PIVOT = Array(ProductVariables.BRAND, ProductVariables.MVP)
  val BRICK_PRICE_BAND_PIVOT = Array(ProductVariables.BRICK, ProductVariables.PRICE_BAND)
  val MVP_COLOR_PIVOT = Array(ProductVariables.MVP, ProductVariables.COLOR)
  val MVP_DISCOUNT_PIVOT = Array(ProductVariables.MVP, Recommendation.DISCOUNT_STATUS)
  val BRAND_MVP_CITY_PIVOT = Array(ProductVariables.BRAND, ProductVariables.MVP, SalesAddressVariables.CITY)
  val BRAND_MVP_STATE_PIVOT = Array(ProductVariables.BRAND, ProductVariables.MVP, Recommendation.RECOMMENDATION_STATE)

  val ORDER_ITEM_DAYS = 30

  val SEARCH_RECOMMENDATION_ORDER_ITEM_DAYS = 180

  val NUM_REC_SKU_REF_SKU = 8

  val DISCOUNT_THRESHOLD = 35

}
