package com.jabong.dap.common.constants.campaign

import com.jabong.dap.common.constants.variables.ProductVariables

/**
 * Created by rahul for recommendation constants  on 9/7/15.
 */
object Recommendation {

  val  LIVE_COMMON_RECOMMENDER = "live_common_recommender"
  val  SALES_ORDER_ITEM_SKU = "sales_order_item_sku"
  val  NUMBER_LAST_30_DAYS_ORDERED = "number_last_30_days_ordered"
  val  LAST_SOLD_DATE = "last_sold_date"

  val  NUM_RECOMMENDATIONS = 20
  val  BRICK_MVP_SUB_TYPE = "brick_mvp"
  
  val INVENTORY_FILTER = "inventory_filter"
  
  val WEEKLY_AVERAGE_SALE = "weekly_average_sale"

  val BRICK_MVP_PIVOT = Array(ProductVariables.BRICK, ProductVariables.MVP)
  val BRAND_MVP_PIVOT = Array(ProductVariables.BRAND, ProductVariables.MVP)
  val ORDER_ITEM_DAYS = 30



}
