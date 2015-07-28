package com.jabong.dap.data.storage

import java.io.File

import com.jabong.dap.common.AppConfig

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {

  val basePath = AppConfig.config.basePath

  val INPUT_PATH = basePath + File.separator + "input"

  val OUTPUT_PATH = AppConfig.config.outputPath
  //  val OUTPUT_PATH = OUTPUT_PATH = basePath + File.separator + "output"

  val EXTRAS = "extras"

  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ADDRESS = "sales_order_address"
  val SALES_RULE = "sales_rule"
  val SALES_CART = "sales_cart"

  //customer
  val RESULT_CUSTOMER_INCREMENTAL = "result_customer_incremental"
  val RESULT_CUSTOMER_OLD = "result_customer_old"

  val CUSTOMER = "customer"
  val CUSTOMER_STORECREDITS_HISTORY = "customer_storecredits_history"
  val CUSTOMER_SEGMENTS = "customer_segments"
  val CUSTOMER_PRODUCT_SHORTLIST = "customer_product_shortlist"
  val RESULT_CUSTOMER_PRODUCT_SHORTLIST = "result_customer_product_shortlist"

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

  //Clickstream
  val CLICKSTREAM = "clickstream"

  //Ad4Push customer response
  val AD4PUSH = "ad4push"

  //non schema constants for ad4push
  val ANDROID_CSV_PREFIX = "exportMessagesReactions_517_"
  val IPHONE_CSV_PREFIX = "exportMessagesReactions_515_"

  val REACTIONS_IOS = "reactions_ios"
  val REACTIONS_ANDROID = "reactions_android"
  val REACTIONS_IOS_CSV = "reactions_ios_csv"
  val REACTIONS_ANDROID_CSV = "reactions_android_csv"

  val IOS = "ios"
  val WINDOWS = "windows"
  val ANDROID = "android"

  // modes for reading data
  val FULL_MERGE_MODE = "full_merge"
  val FULL_FETCH_MODE = "full_fetch"

  // modes for reading and writing data
  val DAILY_MODE = "daily"
  val MONTHLY_MODE = "monthly"
  val HOURLY_MODE = "hourly"

  // modes for writing data
  val FULL = "full"

  val HISTORICAL = "historical"

  // File formats
  val CSV = "csv"
  val PARQUET = "parquet"
  val ORC = "orc"
  val JSON = "json"

  // File Save modes
  val IGNORE_SAVEMODE = "Ignore"
  val ERROR_SAVEMODE = "ErrorIfExists"
  val OVERWRITE_SAVEMODE = "Overwrite"
  val APPEND_SAVEMODE = "Append"

  //DB Server
  val MYSQL = "mysql"
  val SQLSERVER = "sqlserver"

  // Data sources
  val BOB = "bob"
  val ERP = "erp"
  val UNICOMMERCE = "unicommerce"
  val NEXTBEE = "nextbee"

  val DEVICE_MAPPING = "device_mapping"
  val USER_DEVICE_MAP_APP = "userDeviceMapApp"

  val CUSTOMER_DEVICE_MAPPING = "customerDeviceMapping"

  val CAMPAIGN = "campaign"

  val ITEM_ON_DISCOUNT = "item_on_discount"

  val LOW_STOCK = "low_stock"

  val SKU_SELECTION = "sku_selection"

  val ITR_30_DAY_DATA = "itr_30_day_data"

  val YESTERDAY_ITR_DATA = "yesterday_itr_data"

}
