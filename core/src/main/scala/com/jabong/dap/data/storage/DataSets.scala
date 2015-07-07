package com.jabong.dap.data.storage

import com.jabong.dap.common.AppConfig

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {
  val basePath = AppConfig.config.basePath

  val BOB_PATH = basePath + "bob/"
  val VARIABLE_PATH = basePath + "variables/"

  val TEST_RESOURCES = "src/test/resources/"

  //read prequet file from these paths

  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ADDRESS = "sales_order_address"
  val SALES_RULE = "sales_rule"

  //customer
  val CUSTOMER = "customer"
  val CUSTOMER_STORECREDITS_HISTORY = "customer_storecredits_history"
  val CUSTOMER_SEGMENTS = "customer_segments"

  //newsletter
  val NEWSLETTER_SUBSCRIPTION = "newsletter_subscription"

  //catalog
  val CATALOG_CONFIG = "catalog_config"
  val CATALOG_BRAND = "catalog_brand"

}
