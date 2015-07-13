package com.jabong.dap.common.constants.variables

/**
 * Created by mubarak on 6/7/15.
 * Sales order item variables
 */
object SalesOrderItemVariables {
  val ID_SALES_ORDER_ITEM = "id_sales_order_item"
  val FK_SALES_ORDER = "fk_sales_order"
  val FK_SALES_ORDER_ITEM_STATUS = "fk_sales_order_item_status"
  val CREATED_AT = "created_at"
  val UPDATED_AT = "updated_at"
  val ORDERS_COUNT = "orders_count"
  val PAID_PRICE = "paid_price"
  val GIFTCARD_CREDITS_VALUE = "giftcard_credits_value"
  val STORE_CREDITS_VALUE = "store_credits_value"
  val PAYBACK_CREDITS_VALUE = "payback_credits_value"
  val FILTER_APP = "domain ='android' or domain = 'ios' or domain ='windows'"
  val FILTER_WEB = "domain ='w'"
  val FILTER_MWEB = "domain ='m'"
  val APP = "/app/"
  val WEB = "/web/"
  val MOBILE_WEB = "/mobile_web/"
  val UNIT_PRICE = "unit_price"
  val SALES_ORDER_ITEM_STATUS = "fk_sales_order_item_status"
  val REVENUE = "revenue"
  val REVENUE_APP = "revenue_app"
  val REVENUE_WEB = "revenue_web"
  val REVENUE_MWEB = "revenue_mweb"
  val ORDERS_COUNT_APP = "orders_count_app"
  val ORDERS_COUNT_WEB = "orders_count_web"
  val ORDERS_COUNT_MWEB = "orders_count_mweb"
  val FILTER_SUCCESSFUL_ORDERS = "fk_sales_order_item_status = 3 or fk_sales_order_item_status = 4 or fk_sales_order_item_status = 5 or fk_sales_order_item_status = 6 or fk_sales_order_item_status = 7 or fk_sales_order_item_status = 11 or fk_sales_order_item_status = 17 or fk_sales_order_item_status = 24 or fk_sales_order_item_status = 33 or fk_sales_order_item_status = 34"
  val ORDERS_COUNT_SUCCESSFUL = "orders_count_successful"

  val REVENUE_APP_LIFE = "revenue_app_life"
  val REVENUE_WEB_LIFE = "revenue_web_life"
  val REVENUE_MWEB_LIFE = "revenue_mweb_life"
  val ORDERS_COUNT_APP_LIFE = "orders_count_app_life"
  val ORDERS_COUNT_WEB_LIFE = "orders_count_web_life"
  val ORDERS_COUNT_MWEB_LIFE = "orders_count_mweb_life"
  val ORDERS_COUNT_LIFE = "oredrs_count_life"


  val REVENUE_7 = "revenue_7"
  val REVENUE_30 = "revenue_30"
  val REVENUE_LIFE = "revenue_life"
  val SUCCESSFUL_ORDERS = "successful_orders"

}
