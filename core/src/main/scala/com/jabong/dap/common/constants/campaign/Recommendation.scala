package com.jabong.dap.common.constants.campaign

import com.jabong.dap.common.constants.variables.ProductVariables

/**
 * Created by rahul for recommendation constants  on 9/7/15.
 */
object Recommendation {

  val LIVE_COMMON_RECOMMENDER = "live_common_recommender"
  val SALES_ORDER_ITEM_SKU = "sales_order_item_sku"
  val NUMBER_LAST_30_DAYS_ORDERED = "number_last_30_days_ordered"
  val LAST_SOLD_DATE = "last_sold_date"

  val NUM_RECOMMENDATIONS = 20
  val BRICK_MVP_SUB_TYPE = "brick_mvp"

  val BRAND_MVP_SUB_TYPE = "brand_mvp"

  val MVP_DISCOUNT = "mvp_discount"
  val ALL = "all"

  val MVP_COLOR = "mvp_color"

  val BRICK_PRICE_BAND_SUB_TYPE = "brick_price_band"

  val INVENTORY_FILTER = "inventory_filter"

  val WEEKLY_AVERAGE_SALE = "weekly_average_sale"

  val DISCOUNT_STATUS = "discount_status"

  val BRICK_MVP_PIVOT = Array(ProductVariables.BRICK, ProductVariables.MVP)
  val BRAND_MVP_PIVOT = Array(ProductVariables.BRAND, ProductVariables.MVP)
  val BRICK_PRICE_BAND_PIVOT = Array(ProductVariables.BRICK, ProductVariables.PRICE_BAND)
  val MVP_COLOR_PIVOT = Array(ProductVariables.MVP, ProductVariables.COLOR)
  val MVP_DISCOUNT_PIVOT = Array(ProductVariables.MVP, Recommendation.DISCOUNT_STATUS)

  val ORDER_ITEM_DAYS = 30

  val NUM_REC_SKU_REF_SKU = 8

  val DISCOUNT_THRESHOLD = 35

  val NTHDAY_45 = "nthday_45"
  val NTHDAY_60 = "nthday_60"


}
