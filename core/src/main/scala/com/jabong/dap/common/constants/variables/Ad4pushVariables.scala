package com.jabong.dap.common.constants.variables

/**
 * Created by mubarak on 21/8/15.
 */
object Ad4pushVariables {

  var DEVICE_ID = "device_id"
  var UDID = "UDID"
  var TOKEN = "token"
  var OPENCOUNT = "opencount"
  var FIRSTOPEN = "firstopen"
  var LASTOPEN = "lastopen"
  var MODEL = "model"
  var VERSION = "version"
  var LANGUAGE = "language"
  var BUNDLEVERSION = "bundleversion"
  var LAT = "lat"
  var LON = "lon"
  var ALTITUDE = "altitude"
  var GEOLOCATION_CREATED = "geolocationCreated"
  var VERSIONSDK = "versionsdk"
  var FEEDBACK = "feedback"
  var TIME_ZONE = "time_zone"
  var SYSTEM_OPTIN_NOTIFS = "system_optin_notifs"
  var ENABLED_NOTIFS = "enabled_notifs"
  var ENABLED_INAPPS = "enabled_inapps"
  var RANDOMID = "randomId"
  var COUNTRYCODE = "countryCode"
  var AGGREGATED_NUMBER_OF_PURCHASES = "aggregatedNumberOfPurchases"
  var GENDER = "gender"
  var HAS_SHARED_PRODUCT = "hasSharedProduct"
  var LAST_ABANDONED_CART_DATE = "lastAbandonedCartDate"
  var LAST_ABANDONED_CART_PRODUCT = "lastAbandonedCartProduct"
  var LASTORDERDATE = "lastOrderDate"
  var LAST_SEARCH = "lastSearch"
  var LAST_SEARCH_DATE = "lastSearchDate"
  var LEAD = "lead"
  var LOGIN_USER_ID = "loginUserID"
  var MOST_VISITED_CATEGORY = "mostVisitedCategory"
  var ORDER_STATUS = "orderStatus"
  var PURCHASE = "purchase"
  var REGISTRATION = "registration"
  var STATUS_IN_APP = "statusInApp"
  var WISHLIST_STATUS = "wishlistStatus"
  var WISHLIST_ADD = "wishlist_add"
  var SHOP_COUNTRY = "shopCountry"
  var AMOUNT_BASKET = "amount_basket"
  var CART = "cart"
  var SPECIFIC_CATEGORY_VISIT_COUNT = "specific_category_visit_count"
  var USER_NAME = "userName"
  var LAST_VIEWED_CATEGORY = "LastViewedCategory"
  var MAX_VISITED_CATEGORY = "maxVisitedCategory"
  var MOST_VISITED_COUNTS = "mostVisitedCounts"

  //Extra fields for 515
  var IDFA = "IDFA"
  var LAST_ORDER_DATE = "last_order_date" //for 515 lastOrderDate coming 0000-00-00 00:00:00
  var SEARCH_DATE = "search_date" //search_date coming as 0000-00-00 00:00:00 where as lastSearchDate coming as 2020-07-27 00:00:00
  var WISHLIST_PRODUCTS_COUNT = "wishlist_products_count"
  var RATED = "rated"

  //Devices Reactions CSV fields
  val MESSAGE_ID = "message_id"
  val CAMPAIGN_ID = "campaign_id"
  val BOUNCE = "bounce"
  val REACTION = "reaction"

  //DeviceReaction variables Full
  val CUSTOMER_ID = "customer_id"
  val LAST_CLICK_DATE = "last_click_date"
  val CLICK_7 = "click_7"
  val CLICK_15 = "click_15"
  val CLICK_30 = "click_30"
  val CLICK_LIFETIME = "click_lifetime"
  val CLICK_MONDAY = "click_monday"
  val CLICK_TUESDAY = "click_tuesday"
  val CLICK_WEDNESDAY = "click_wednesday"
  val CLICK_THURSDAY = "click_thursday"
  val CLICK_FRIDAY = "click_friday"
  val CLICK_SATURDAY = "click_saturday"
  val CLICK_SUNDAY = "click_sunday"
  val CLICKED_TWICE = "clicked_twice"
  val MOST_CLICK_DAY = "most_click_day"

  //working constants
  val EFFECTIVE_7_DAYS = "effective_7_days"
  val EFFECTIVE_15_DAYS = "effective_15_days"
  val EFFECTIVE_30_DAYS = "effective_30_days"
  val MONDAY = "monday"
  val TUESDAY = "tuesday"
  val WEDNESDAY = "wednesday"
  val THURSDAY = "thursday"
  val FRIDAY = "friday"
  val SATURDAY = "saturday"
  val SUNDAY = "sunday"
  val CLICK_ = "click_"

  // DeviceReaction variables Incremental
  val CLICKED_TODAY = "clicked_today"
  val CLICKED_7_DAYS = "clicked_7_days"
  val CLICKED_15_DAYS = "clicked_15_days"
  val CLICKED_30_DAYS = "clicked_30_days"

}
