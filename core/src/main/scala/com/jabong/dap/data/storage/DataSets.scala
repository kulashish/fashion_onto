package com.jabong.dap.data.storage

/**
 * Created by jabong on 28/5/15.
 */
object DataSets {
  //sales
  val SALES_ORDER = "sales_order"
  val SALES_ORDER_ITEM = "sales_order_item"
  val SALES_ORDER_ADDRESS = "sales_order_address"
  val SALES_RULE = "sales_rule"
  val SALES_CART = "sales_cart"
  val SALES_RULE_SET = "sales_rule_set"
  val CATALOG_SHOP_LOOK_DETAIL = "catalog_shop_look_detail"

  val PAYMENT_PREPAID_TRANSACTION_DATA = "payment_prepaid_transaction_data"
  val PAYMENT_BANK_PRIORITY = "payment_bank_priority"

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
  val PRICING_SKU_DATA = "pricingSKUData"

  // precalculated values for email campaigns
  val SUCCESSFUL_ORDERS_COUNT = "successfulOrdersCount"
  val FAV_BRAND = "favBrand"

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
  val RESPONSYS = "responsys"

  val DEVICE_MAPPING = "device_mapping"
  val CUSTOMER_MASTER_RECORD_FEED = "customerMasterRecordFeed"

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
  val MAPS = "maps"

  //item master
  val ITEM_MASTER_COMPLETE_DUMP = "item_master_complete_dump"

  //DCF FEEDS
  val CLICKSTREAM_MERGED_FEED = "clickstream_merged_feed"
  val DCF_FEED_FILENAME = "webhistory_"
  val DCF_FEED_GENERATE = "dcfFeedGenerate"

  //Campaign Quality
  val CAMPAIGN_QUALITY = "campaignQuality"

  //recommendations
  val RECOMMENDATIONS = "recommendations"
  val BRICK_MVP_RECOMMENDATIONS = "brick_mvp"
  val BRICK_MVP_SEARCH_RECOMMENDATIONS = "brick_mvp_search"
  val BRAND_MVP_RECOMMENDATIONS = "brand_mvp"
  val BRICK_PRICE_BAND_RECOMMENDATIONS = "brick_price_band"
  val MVP_COLOR_RECOMMENDATIONS = "mvp_color"
  val MVP_DISCOUNT_RECOMMENDATIONS = "mvp_discount"
  val BRAND_MVP_CITY_RECOMMENDATIONS = "brand_mvp_city"
  val BRAND_MVP_STATE_RECOMMENDATIONS = "brand_mvp_state"

  // contact list
  val CONTACT_LIST_MOBILE = "contactListMobile"
  val CUSTOMER_PREFERRED_TIMESLOT_PART2 = "customerPreferredTimeslotPart2"
  val CUSTOMER_PREFERRED_TIMESLOT_PART1 = "customerPreferredTimeslotPart1"
  val CUST_TOP5 = "custTop5"
  val CUSTOMER_ORDERS = "customerOrders"
  val CUSTOMER_APP_DETAILS = "customerAppDetails"
  val NL_DATA_LIST = "NL_data_list"
  val CONTACT_LIST_PLUS = "Contact_list_Plus"
  val APP_EMAIL_FEED = "app_email_feed"
  val CUSTOMER_JC_DETAILS = "customerJCDetails"
  val PAYBACK_DATA = "paybackData"
  val SALES_ORDER_ADDR_FAV = "salesOrderAddrFav"
  val DND = "DND"
  val SMS_DELIVERED = "sms_delivered"
  val SMS_OPT_OUT = "sms_opt_out"
  val BLOCK_LIST_NUMBERS = "block_list_numbers"
  val ZONE_CITY = "zone_city"
  val CUST_PREFERENCE = "custPreference"
  val CUST_WELCOME_VOUCHER = "custWelcomeVoucher"
  val DND_MERGER = "dndMerger"
  val SMS_OPT_OUT_MERGER = "smsOptOutMerger"
  val SOLUTIONS_INFINITI = "solutionsInfiniti"
  val SHOP_THE_LOOK = "shopTheLook"

  // type of campaigns
  val PUSH_CAMPAIGNS = "push_campaigns"
  val EMAIL_CAMPAIGNS = "email_campaigns"
  val CALENDAR_CAMPAIGNS = "calendar_campaigns"

  val AD4PUSH_ID = "ad4pushId"
  //Clickstream Data Quality

  val CLICKSTREAM_DATA_QUALITY = "clickstreamDataQualityCheck"

  val CLICKSTREAM_YESTERDAY_SESSION = "clickstreamYesterdaySession"

  val CLICKSTREAM_SURF3_VARIABLE = "clickstreamSurf3Variable"

  val SURF1_PROCESSED_VARIABLE = "Surf1ProcessedVariable"

  val CUSTOMER_SURF_AFFINITY = "customerSurfAffinity"

  val CLICKSTREAM_SURF3_MERGED_DATA30 = "clickstreamSurf3MergeData30"

  val OPEN = "open"
  val CLICK = "click"
  val CUST_EMAIL_RESPONSE = "custEmailResponse"

  //SalesOrderItemVariables-->Customerorders.csv
  val SALES_ITEM_REVENUE = "salesItemRevenue"
  val SALES_ITEM_COUPON_DISC = "sales_item_coupon_disc"
  val SALES_ITEM_INVALID_CANCEL = "sales_order_invalid_cancel"
  val SALES_ITEM_CAT_BRICK_PEN = "sales_item_cat_brick_pen"
  val SALES_ITEM_ORDERS_VALUE = "sales_item_orders_value"
  val SALES_ADDRESS_FIRST = "sales_address_first"
  val CAT_COUNT = "cat_count"
  val CAT_AVG = "cat_avg"

  // campaigns
  val ACART_HOURLY = "acartHourly"

  // miscellaneous data sets
  val CITY_WISE_DATA = "cityWiseData"
  val WINBACK_CUSTOMER = "winbackCustomer"

  val MONGO_FEED_GENERATOR = "mongoFeedGenerator"

  //CRM tables
  val CRM_TicketDetails = "CRM_TicketDetails"
  val CRM_TicketMaster = "CRM_TicketMaster"
  val CRM_TicketStatusLog = "CRM_TicketStatusLog"
}

