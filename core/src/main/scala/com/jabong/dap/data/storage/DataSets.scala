package com.jabong.dap.data.storage

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {

  val EXTRAS = "extras"

  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ADDRESS = "sales_order_address"
  val SALES_RULE = "sales_rule"
  val SALES_CART = "sales_cart"

  //customer
  val CUSTOMER = "customer"
  val CUSTOMER_STORECREDITS_HISTORY = "customer_storecredits_history"
  val CUSTOMER_SEGMENTS = "customer_segments"
  val CUSTOMER_PRODUCT_SHORTLIST = "customer_product_shortlist"

  //newsletter
  val NEWSLETTER_SUBSCRIPTION = "newsletter_subscription"

  //catalog
  val CATALOG_CONFIG = "catalog_config"
  val CATALOG_BRAND = "catalog_brand"
  val CATALOG_SIMPLE = "catalog_simple"
  val CATALOG_SUPPLIER = "catalog_supplier"
  val CATALOG_STOCK = "catalog_stock"
  val CATALOG_PRODUCT_IMAGE = "catalog_product_image"
  val CATALOG_CATEGORY = "catalog_category"
  val CATALOG_CONFIG_HAS_CATALOG_CATEGORY = "catalog_config_has_catalog_category"

  //PaybackCustomer
  val SALES_ORDER_PAYBACK_EARN = "sales_order_payback_earn"
  val SALES_ORDER_PAYBACK_REDEEM = "sales_order_payback_redeem"

  //Clickstream
  val CLICKSTREAM = "clickstream"

  //Ad4Push customer response
  val AD4PUSH = "ad4push"

  val SKU_DATA = "sku_data"
  val PRICING = "pricing"
  //non schema constants for ad4push
  val ANDROID_CSV_PREFIX = "exportMessagesReactions_517_"
  val IPHONE_CSV_PREFIX = "exportMessagesReactions_515_"

  val REACTIONS_IOS = "reactions_ios"
  val REACTIONS_ANDROID = "reactions_android"
  val CUSTOMER_RESPONSE = "customer_response"

  val IOS = "ios"
  val WINDOWS = "windows"
  val ANDROID = "android"
  val DESKTOP = "w"
  val MOBILEWEB = "m"
  val WSOA = "wsoa"

  val WEB = "web"
  val APP = "app"

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
  val BASIC_ITR = "basicITR"

  val CAMPAIGNS = "campaigns"

  //item master
  val ITEM_MASTER_COMPLETE_DUMP = "item_master_complete_dump"

  //DCF FEEDS
  val DCF_INPUT_MERGED_HIVE_TABLE = "merge.merge_pagevisit"
  val DCF_FEED = "dcffeed"
  val CLICKSTREAM_MERGED_FEED = "clickstream_merged_feed"
  val DCF_FEED_FILENAME = "webhistory_"
}
