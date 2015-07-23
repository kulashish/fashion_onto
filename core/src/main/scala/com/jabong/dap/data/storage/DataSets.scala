package com.jabong.dap.data.storage

import java.io.File

import com.jabong.dap.common.AppConfig

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {

  val basePath = AppConfig.config.basePath

  val INPUT_PATH = basePath + File.separator + "input"
  val OUTPUT_PATH = basePath + File.separator + "output"

  val TEST_RESOURCES = "src" + File.separator + "test" + File.separator + "resources" + File.separator

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

  //Ad4Push customer response
  val AD4PUSH = "ad4Push"

  //non schema constants for ad4push
  val IPHONE_CSV_PREFIX = "exportMessagesReactions_517_"
  val ANDROID_CSV_PREFIX = "exportMessagesReactions_515_"

  val REACTION_IOS = "reaction_ios"
  val REACTION_ANDROID = "reaction_android"

  // modes for reading data
  val FULL_MERGE_MODE = "full_merge"
  val FULL_FETCH_MODE = "full_fetch"

  // modes for reading and writing data
  val DAILY_MODE = "daily"
  val MONTHLY_MODE = "monthly"
  val HOURLY_MODE = "hourly"

  // modes for writing data
  val FULL = "full"

  val CSV = "csv"
  val PARQUET = "parquet"
  val ORC = "orc"

  val IGNORE_SAVEMODE = "ignore"
  val ERROR_SAVEMODE = "error"
  val OVERWRITE_SAVEMODE = "overwrite"
  val APPEND_SAVEMODE = "append"

  val BOB = "bob"
  val ERP = "erp"
  val UNICOMMERCE = "unicommerce"
  val NEXTBEE = "nextbee"

}