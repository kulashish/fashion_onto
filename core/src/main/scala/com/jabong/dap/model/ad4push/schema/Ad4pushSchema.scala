package com.jabong.dap.model.ad4push.schema

import com.jabong.dap.common.constants.variables.CustomerVariables
import org.apache.spark.sql.types._

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
object Ad4pushSchema {
  //CSV schema
  val schemaCsv = StructType(Array(
    StructField(CustomerVariables.LOGIN_USER_ID, StringType, true),
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.MESSAGE_ID, StringType, true),
    StructField(CustomerVariables.CAMPAIGN_ID, StringType, true),
    StructField(CustomerVariables.BOUNCE, IntegerType, true),
    StructField(CustomerVariables.REACTION, IntegerType, true)
  ))

  val dfFromCsv = StructType(Array(
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.MESSAGE_ID, StringType, true),
    StructField(CustomerVariables.CAMPAIGN_ID, StringType, true),
    StructField(CustomerVariables.BOUNCE, IntegerType, true),
    StructField(CustomerVariables.REACTION, IntegerType, true)
  ))
  //reduced SCHEMA from CSV
  val reducedDF = StructType(Array(
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.REACTION, IntegerType, true)
  ))

  //full effective schema
  val effectiveDF = StructType(Array(
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.EFFECTIVE_7_DAYS, IntegerType, false),
    StructField(CustomerVariables.EFFECTIVE_15_DAYS, IntegerType, false),
    StructField(CustomerVariables.EFFECTIVE_30_DAYS, IntegerType, false),
    StructField(CustomerVariables.CLICKED_TODAY, IntegerType, false)
  ))

  //effective schema with 7, 15 days before
  val joined_7_15 = StructType(Array(
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.EFFECTIVE_7_DAYS, IntegerType, true),
    StructField(CustomerVariables.EFFECTIVE_15_DAYS, IntegerType, true)
  ))

  //effective schema with 7, 15, 30 days before
  val joined_7_15_30 = StructType(Array(
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.EFFECTIVE_7_DAYS, IntegerType, true),
    StructField(CustomerVariables.EFFECTIVE_15_DAYS, IntegerType, true),
    StructField(CustomerVariables.EFFECTIVE_30_DAYS, IntegerType, true)
  ))

  //Final result schema
  val deviceReaction = StructType(Array(
    StructField(CustomerVariables.DEVICE_ID, StringType, true),
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.LAST_CLICK_DATE, StringType, true),
    StructField(CustomerVariables.CLICK_7, IntegerType, false),
    StructField(CustomerVariables.CLICK_15, IntegerType, false),
    StructField(CustomerVariables.CLICK_30, IntegerType, false),
    StructField(CustomerVariables.CLICK_LIFETIME, IntegerType, false),
    StructField(CustomerVariables.CLICK_MONDAY, IntegerType, false),
    StructField(CustomerVariables.CLICK_TUESDAY, IntegerType, false),
    StructField(CustomerVariables.CLICK_WEDNESDAY, IntegerType, false),
    StructField(CustomerVariables.CLICK_THURSDAY, IntegerType, false),
    StructField(CustomerVariables.CLICK_FRIDAY, IntegerType, false),
    StructField(CustomerVariables.CLICK_SATURDAY, IntegerType, false),
    StructField(CustomerVariables.CLICK_SUNDAY, IntegerType, false),
    StructField(CustomerVariables.CLICKED_TWICE, IntegerType, false),
    StructField(CustomerVariables.MOST_CLICK_DAY, StringType, true)
  ))

  val Ad4pushDeviceAndroid = StructType(Array(
    StructField(CustomerVariables.UDID, StringType, true),
    StructField(CustomerVariables.TOKEN, StringType, true),
    StructField(CustomerVariables.OPENCOUNT, StringType, true),
    StructField(CustomerVariables.FIRSTOPEN, StringType, true),
    StructField(CustomerVariables.LASTOPEN, StringType, true),
    StructField(CustomerVariables.MODEL, StringType, true),
    StructField(CustomerVariables.VERSION, StringType, true),
    StructField(CustomerVariables.LANGUAGE, StringType, true),
    StructField(CustomerVariables.BUNDLEVERSION, StringType, true),
    StructField(CustomerVariables.LAT, StringType, true),
    StructField(CustomerVariables.LON, StringType, true),
    StructField(CustomerVariables.ALTITUDE, StringType, true),
    StructField(CustomerVariables.GEOLOCATION_CREATED, StringType, true),
    StructField(CustomerVariables.VERSIONSDK, StringType, true),
    StructField(CustomerVariables.FEEDBACK, StringType, true),
    StructField(CustomerVariables.TIME_ZONE, StringType, true),
    StructField(CustomerVariables.SYSTEM_OPTIN_NOTIFS, StringType, true),
    StructField(CustomerVariables.ENABLED_NOTIFS, StringType, true),
    StructField(CustomerVariables.ENABLED_INAPPS, StringType, true),
    StructField(CustomerVariables.RANDOMID, StringType, true),
    StructField(CustomerVariables.COUNTRYCODE, StringType, true),
    StructField(CustomerVariables.AGGREGATED_NUMBER_OF_PURCHASES, StringType, true),
    StructField(CustomerVariables.GENDER, StringType, true),
    StructField(CustomerVariables.HAS_SHARED_PRODUCT, StringType, true),
    StructField(CustomerVariables.LAST_ABANDONED_CART_DATE, StringType, true),
    StructField(CustomerVariables.LAST_ABANDONED_CART_PRODUCT, StringType, true),
    StructField(CustomerVariables.LASTORDERDATE, StringType, true),
    StructField(CustomerVariables.LAST_SEARCH, StringType, true),
    StructField(CustomerVariables.LAST_SEARCH_DATE, StringType, true),
    StructField(CustomerVariables.LEAD, StringType, true),
    StructField(CustomerVariables.LOGIN_USER_ID, StringType, true),
    StructField(CustomerVariables.MOST_VISITED_CATEGORY, StringType, true),
    StructField(CustomerVariables.ORDER_STATUS, StringType, true),
    StructField(CustomerVariables.PURCHASE, StringType, true),
    StructField(CustomerVariables.REGISTRATION, StringType, true),
    StructField(CustomerVariables.STATUS_IN_APP, StringType, true),
    StructField(CustomerVariables.WISHLIST_STATUS, StringType, true),
    StructField(CustomerVariables.WISHLIST_ADD, StringType, true),
    StructField(CustomerVariables.SHOP_COUNTRY, StringType, true),
    StructField(CustomerVariables.AMOUNT_BASKET, StringType, true),
    StructField(CustomerVariables.CART, StringType, true),
    StructField(CustomerVariables.SPECIFIC_CATEGORY_VISIT_COUNT, StringType, true),
    StructField(CustomerVariables.USER_NAME, StringType, true),
    StructField(CustomerVariables.LAST_VIEWED_CATEGORY, StringType, true),
    StructField(CustomerVariables.MAX_VISITED_CATEGORY, StringType, true),
    StructField(CustomerVariables.MOST_VISITED_COUNTS, StringType, true)
  ))

  val Ad4pushDeviceIOS = StructType(Array(
    StructField(CustomerVariables.UDID, StringType, true),
    StructField(CustomerVariables.TOKEN, StringType, true),
    StructField(CustomerVariables.OPENCOUNT, StringType, true),
    StructField(CustomerVariables.FIRSTOPEN, StringType, true),
    StructField(CustomerVariables.LASTOPEN, StringType, true),
    StructField(CustomerVariables.MODEL, StringType, true),
    StructField(CustomerVariables.VERSION, StringType, true),
    StructField(CustomerVariables.LANGUAGE, StringType, true),
    StructField(CustomerVariables.BUNDLEVERSION, StringType, true),
    StructField(CustomerVariables.LAT, StringType, true),
    StructField(CustomerVariables.LON, StringType, true),
    StructField(CustomerVariables.ALTITUDE, StringType, true),
    StructField(CustomerVariables.GEOLOCATION_CREATED, StringType, true),
    StructField(CustomerVariables.VERSIONSDK, StringType, true),
    StructField(CustomerVariables.FEEDBACK, StringType, true),
    StructField(CustomerVariables.TIME_ZONE, StringType, true),
    StructField(CustomerVariables.SYSTEM_OPTIN_NOTIFS, StringType, true),
    StructField(CustomerVariables.ENABLED_NOTIFS, StringType, true),
    StructField(CustomerVariables.ENABLED_INAPPS, StringType, true),
    StructField(CustomerVariables.RANDOMID, StringType, true),
    StructField(CustomerVariables.COUNTRYCODE, StringType, true),
    StructField(CustomerVariables.AGGREGATED_NUMBER_OF_PURCHASES, StringType, true),
    StructField(CustomerVariables.GENDER, StringType, true),
    StructField(CustomerVariables.HAS_SHARED_PRODUCT, StringType, true),
    StructField(CustomerVariables.LAST_ABANDONED_CART_DATE, StringType, true),
    StructField(CustomerVariables.LAST_ABANDONED_CART_PRODUCT, StringType, true),
    StructField(CustomerVariables.LASTORDERDATE, StringType, true),
    StructField(CustomerVariables.LAST_SEARCH, StringType, true),
    StructField(CustomerVariables.LAST_SEARCH_DATE, StringType, true),
    StructField(CustomerVariables.LEAD, StringType, true),
    StructField(CustomerVariables.LOGIN_USER_ID, StringType, true),
    StructField(CustomerVariables.MOST_VISITED_CATEGORY, StringType, true),
    StructField(CustomerVariables.ORDER_STATUS, StringType, true),
    StructField(CustomerVariables.PURCHASE, StringType, true),
    StructField(CustomerVariables.REGISTRATION, StringType, true),
    StructField(CustomerVariables.STATUS_IN_APP, StringType, true),
    StructField(CustomerVariables.WISHLIST_STATUS, StringType, true),
    StructField(CustomerVariables.WISHLIST_ADD, StringType, true),
    StructField(CustomerVariables.SHOP_COUNTRY, StringType, true),
    StructField(CustomerVariables.AMOUNT_BASKET, StringType, true),
    StructField(CustomerVariables.CART, StringType, true),
    StructField(CustomerVariables.SPECIFIC_CATEGORY_VISIT_COUNT, StringType, true),
    StructField(CustomerVariables.USER_NAME, StringType, true),
    StructField(CustomerVariables.LAST_VIEWED_CATEGORY, StringType, true),
    StructField(CustomerVariables.MAX_VISITED_CATEGORY, StringType, true),
    StructField(CustomerVariables.MOST_VISITED_COUNTS, StringType, true),
    StructField(CustomerVariables.IDFA, StringType, true),
    StructField(CustomerVariables.LAST_ORDER_DATE, StringType, true),
    StructField(CustomerVariables.SEARCH_DATE, StringType, true),
    StructField(CustomerVariables.WISHLIST_PRODUCTS_COUNT, StringType, true),
    StructField(CustomerVariables.RATED, StringType, true)
  ))

}
