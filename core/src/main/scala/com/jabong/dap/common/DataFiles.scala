package com.jabong.dap.common

/**
 * Created by jabong on 28/5/15.
 */
object DataFiles {

  val BOB_PATH = "hdfs://bigdata-master.jabong.com:8020/data/bob/"
  val VARIABLE_PATH = "hdfs://bigdata-master/data/variables/"
  val TEST_RESOURCES = "src/test/resources/"

  //read prequet file from these paths

  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ADDRESS = "sales_order_address"
  val SALES_RULE = "sales_rule"
  val WELCOME1 = "welcome1"
  val WELCOME2 = "welcome2"


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
