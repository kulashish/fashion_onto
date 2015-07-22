package com.jabong.dap.data.storage

import com.jabong.dap.common.AppConfig

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {
  val basePath = AppConfig.config.basePath

  //  val BOB_PATH = basePath + "bob/"
  //  val VARIABLE_PATH = basePath + "variables/"

  val TEST_RESOURCES = "src/test/resources/"

  //read prequet file from these paths

  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ADDRESS = "sales_order_address"
  val SALES_RULE = "sales_rule"

  //customer
  val RESULT_CUSTOMER_INCREMENTAL = "result_customer_incremental"
  val RESULT_CUSTOMER_OLD = "result_customer_old"

  val CUSTOMER = "customer"
  val CUSTOMER_STORECREDITS_HISTORY = "customer_storecredits_history"
  val CUSTOMER_SEGMENTS = "customer_segments"

  //newsletter
  val NEWSLETTER_SUBSCRIPTION = "newsletter_subscription"

  //catalog
  val CATALOG_CONFIG = "catalog_config"
  val CATALOG_BRAND = "catalog_brand"

  //PaybackCustomer
  val PAYBACK_CUSTOMER = "payback_customer"
  val SALES_ORDER_PAYBACK_EARN = "sales_order_payback_earn"
  val SALES_ORDER_PAYBACK_REDEEM = "sales_order_payback_redeem"

  //Newsletter Preferences
  val NEWSLETTER_PREFERENCES = "newsletter_preferences"

  //ad4push
  val DEVICE_REACTION_CSV_DIRECTORY = TEST_RESOURCES+ "ad4push/csv"
  val DEVICE_REACTION_DF_DIRECTORY = TEST_RESOURCES+"ad4push/df"
  val DEVICE_REACTION_JSON_DIRECTORY = TEST_RESOURCES+"ad4push/json"
  //non schema constants for ad4push
  val IPHONE_CSV_PREFIX = "exportMessagesReactions_517_"
  val ANDROID_CSV_PREFIX = "exportMessagesReactions_515_"

  val IPHONE_DF_PREFIX = "iphone_"
  val ANDROID_DF_PREFIX = "android_"


  val BEFORE_7_DAYS_DF_NAME = "Before7days"
  val BEFORE_15_DAYS_DF_NAME = "Before15days"
  val BEFORE_30_DAYS_DF_NAME = "Before30days"
  val FULL_DF_NAME = "Full"
  val CSV_EXTENSION = ".csv"
  val PARQUET_EXTENSION = ".parquet"
}