package com.jabong.dap.data.storage

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {

  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ITEM_ORDERS_COUNT = "sales_order_item/ordersCount"
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

  val SKU_DATA = "sku_data"
  val PRICING = "pricing"

  val DEVICES_IOS = "devices_ios"
  val DEVICES_ANDROID = "devices_android"
  val AD4PUSH_DEVICE_MERGER = "ad4pushDeviceMerger"

  val REACTIONS_IOS = "reactions_ios"
  val REACTIONS_ANDROID = "reactions_android"
  val CUSTOMER_RESPONSE = "customer_response"
  val AD4PUSH_CUSTOMER_RESPONSE = "ad4pushCustomerResponse"

  val IOS_CODE = "515"
  val ANDROID_CODE = "517"

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
  val CRM = "crm"
  val JDARESOURCE = "jDaReSource"
  val UNICOMMERCE = "unicommerce"
  val NEXTBEE = "nextbee"
  val RESPONSYS = "responsys"

  val DEVICE_MAPPING = "device_mapping"

  // Data Outputs
  val USER_DEVICE_MAP_APP = "userDeviceMapApp"
  val CUSTOMER_DEVICE_MAPPING = "customerDeviceMapping"
  val BASIC_ITR = "basicITR"
  //Ad4Push customer response
  val AD4PUSH = "ad4push"
  //Clickstream
  val CLICKSTREAM = "clickstream"
  val CAMPAIGNS = "campaigns"
  val DCF_FEED = "dcf_feed"
  val EXTRAS = "extras"

  val VARIABLES = "variables"

  //item master
  val ITEM_MASTER_COMPLETE_DUMP = "item_master_complete_dump"

  //DCF FEEDS
  val DCF_INPUT_MERGED_HIVE_TABLE = "merge.merge_pagevisit"
  val CLICKSTREAM_MERGED_FEED = "clickstream_merged_feed"
  val DCF_FEED_FILENAME = "webhistory_"

  //Campaign Quality
  val CAMPAIGN_QUALITY = "campaign_quality"

  //recommendations
  val RECOMMENDATIONS = "recommendations"
  val BRICK_MVP_RECOMMENDATIONS = "brick_mvp"
  val BRAND_MVP_RECOMMENDATIONS = "brand_mvp"

  // contact list
  val CONTACT_LIST_MOBILE = "contactListMobile"
  val DND = "DND"
  val SMS_DELIVERED = "sms_delivered"
  val SMS_OPT_OUT = "sms_opt_out"
  val ZONE_CITY = "zone_city"
  val CUST_PREFERENCE = "custPreference"
  val CUST_WELCOME_VOUCHER = "custWelcomeVoucher"
  val DND_MERGER = "dndMerger"
  val SMS_OPT_OUT_MERGER = "smsOptOutMerger"

  // type of campaigns
  val PUSH_CAMPAIGNS = "push_campaigns"
  val EMAIL_CAMPAIGNS = "email_campaigns"

  val AD4PUSH_ID = "ad4pushId"
  //Clickstream Data Quality

  val CLICKSTREAM_DATA_QUALITY = "clickstreamDataQualityCheck"

  val CLICKSTREAM_YESTERDAY_SESSION = "clickstreamYesterdaySession"

  val CLICKSTREAM_SURF3_VARIABLE = "clickstreamSurf3Variable"

  val CLICKSTREAM_SURF3_MERGED_DATA30 = "clickstreamSurf3MergeData30"

}
