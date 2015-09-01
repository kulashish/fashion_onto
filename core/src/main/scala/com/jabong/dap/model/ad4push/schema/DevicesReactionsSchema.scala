package com.jabong.dap.model.ad4push.schema

import com.jabong.dap.common.constants.variables.{ Ad4pushVariables, DevicesReactionsVariables }
import org.apache.spark.sql.types._

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
object DevicesReactionsSchema {
  //CSV schema
  val schemaCsv = StructType(Array(
    StructField(DevicesReactionsVariables.LOGIN_USER_ID, StringType, true),
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.MESSAGE_ID, StringType, true),
    StructField(DevicesReactionsVariables.CAMPAIGN_ID, StringType, true),
    StructField(DevicesReactionsVariables.BOUNCE, IntegerType, true),
    StructField(DevicesReactionsVariables.REACTION, IntegerType, true)
  ))

  val dfFromCsv = StructType(Array(
    StructField(DevicesReactionsVariables.CUSTOMER_ID, StringType, true),
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.MESSAGE_ID, StringType, true),
    StructField(DevicesReactionsVariables.CAMPAIGN_ID, StringType, true),
    StructField(DevicesReactionsVariables.BOUNCE, IntegerType, true),
    StructField(DevicesReactionsVariables.REACTION, IntegerType, true)
  ))
  //reduced SCHEMA from CSV
  val reducedDF = StructType(Array(
    StructField(DevicesReactionsVariables.CUSTOMER_ID, StringType, true),
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.REACTION, IntegerType, true)
  ))

  //full effective schema
  val effectiveDF = StructType(Array(
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.CUSTOMER_ID, StringType, true),
    StructField(DevicesReactionsVariables.EFFECTIVE_7_DAYS, IntegerType, false),
    StructField(DevicesReactionsVariables.EFFECTIVE_15_DAYS, IntegerType, false),
    StructField(DevicesReactionsVariables.EFFECTIVE_30_DAYS, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICKED_TODAY, IntegerType, false)
  ))

  //effective schema with 7, 15 days before
  val joined_7_15 = StructType(Array(
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.CUSTOMER_ID, StringType, true),
    StructField(DevicesReactionsVariables.EFFECTIVE_7_DAYS, IntegerType, true),
    StructField(DevicesReactionsVariables.EFFECTIVE_15_DAYS, IntegerType, true)
  ))

  //effective schema with 7, 15, 30 days before
  val joined_7_15_30 = StructType(Array(
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.CUSTOMER_ID, StringType, true),
    StructField(DevicesReactionsVariables.EFFECTIVE_7_DAYS, IntegerType, true),
    StructField(DevicesReactionsVariables.EFFECTIVE_15_DAYS, IntegerType, true),
    StructField(DevicesReactionsVariables.EFFECTIVE_30_DAYS, IntegerType, true)
  ))

  //Final result schema
  val deviceReaction = StructType(Array(
    StructField(DevicesReactionsVariables.DEVICE_ID, StringType, true),
    StructField(DevicesReactionsVariables.CUSTOMER_ID, StringType, true),
    StructField(DevicesReactionsVariables.LAST_CLICK_DATE, StringType, true),
    StructField(DevicesReactionsVariables.CLICK_7, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_15, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_30, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_LIFETIME, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_MONDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_TUESDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_WEDNESDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_THURSDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_FRIDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_SATURDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICK_SUNDAY, IntegerType, false),
    StructField(DevicesReactionsVariables.CLICKED_TWICE, IntegerType, false),
    StructField(DevicesReactionsVariables.MOST_CLICK_DAY, StringType, true)
  ))

  val Ad4pushDeviceAndroid = StructType(Array(
    StructField(Ad4pushVariables.UDID, StringType, true),
    StructField(Ad4pushVariables.TOKEN, StringType, true),
    StructField(Ad4pushVariables.OPENCOUNT, StringType, true),
    StructField(Ad4pushVariables.FIRSTOPEN, StringType, true),
    StructField(Ad4pushVariables.LASTOPEN, StringType, true),
    StructField(Ad4pushVariables.MODEL, StringType, true),
    StructField(Ad4pushVariables.VERSION, StringType, true),
    StructField(Ad4pushVariables.LANGUAGE, StringType, true),
    StructField(Ad4pushVariables.BUNDLEVERSION, StringType, true),
    StructField(Ad4pushVariables.LAT, StringType, true),
    StructField(Ad4pushVariables.LON, StringType, true),
    StructField(Ad4pushVariables.ALTITUDE, StringType, true),
    StructField(Ad4pushVariables.GEOLOCATION_CREATED, StringType, true),
    StructField(Ad4pushVariables.VERSIONSDK, StringType, true),
    StructField(Ad4pushVariables.FEEDBACK, StringType, true),
    StructField(Ad4pushVariables.TIME_ZONE, StringType, true),
    StructField(Ad4pushVariables.SYSTEM_OPTIN_NOTIFS, StringType, true),
    StructField(Ad4pushVariables.ENABLED_NOTIFS, StringType, true),
    StructField(Ad4pushVariables.ENABLED_INAPPS, StringType, true),
    StructField(Ad4pushVariables.RANDOMID, StringType, true),
    StructField(Ad4pushVariables.COUNTRYCODE, StringType, true),
    StructField(Ad4pushVariables.AGGREGATED_NUMBER_OF_PURCHASES, StringType, true),
    StructField(Ad4pushVariables.GENDER, StringType, true),
    StructField(Ad4pushVariables.HAS_SHARED_PRODUCT, StringType, true),
    StructField(Ad4pushVariables.LAST_ABANDONED_CART_DATE, StringType, true),
    StructField(Ad4pushVariables.LAST_ABANDONED_CART_PRODUCT, StringType, true),
    StructField(Ad4pushVariables.LASTORDERDATE, StringType, true),
    StructField(Ad4pushVariables.LAST_SEARCH, StringType, true),
    StructField(Ad4pushVariables.LAST_SEARCH_DATE, StringType, true),
    StructField(Ad4pushVariables.LEAD, StringType, true),
    StructField(Ad4pushVariables.LOGIN_USER_ID, StringType, true),
    StructField(Ad4pushVariables.MOST_VISITED_CATEGORY, StringType, true),
    StructField(Ad4pushVariables.ORDER_STATUS, StringType, true),
    StructField(Ad4pushVariables.PURCHASE, StringType, true),
    StructField(Ad4pushVariables.REGISTRATION, StringType, true),
    StructField(Ad4pushVariables.STATUS_IN_APP, StringType, true),
    StructField(Ad4pushVariables.WISHLIST_STATUS, StringType, true),
    StructField(Ad4pushVariables.WISHLIST_ADD, StringType, true),
    StructField(Ad4pushVariables.SHOP_COUNTRY, StringType, true),
    StructField(Ad4pushVariables.AMOUNT_BASKET, StringType, true),
    StructField(Ad4pushVariables.CART, StringType, true),
    StructField(Ad4pushVariables.SPECIFIC_CATEGORY_VISIT_COUNT, StringType, true),
    StructField(Ad4pushVariables.USER_NAME, StringType, true),
    StructField(Ad4pushVariables.LAST_VIEWED_CATEGORY, StringType, true),
    StructField(Ad4pushVariables.MAX_VISITED_CATEGORY, StringType, true),
    StructField(Ad4pushVariables.MOST_VISITED_COUNTS, StringType, true)
  ))

  val Ad4pushDeviceIOS = StructType(Array(
    StructField(Ad4pushVariables.UDID, StringType, true),
    StructField(Ad4pushVariables.TOKEN, StringType, true),
    StructField(Ad4pushVariables.OPENCOUNT, StringType, true),
    StructField(Ad4pushVariables.FIRSTOPEN, StringType, true),
    StructField(Ad4pushVariables.LASTOPEN, StringType, true),
    StructField(Ad4pushVariables.MODEL, StringType, true),
    StructField(Ad4pushVariables.VERSION, StringType, true),
    StructField(Ad4pushVariables.LANGUAGE, StringType, true),
    StructField(Ad4pushVariables.BUNDLEVERSION, StringType, true),
    StructField(Ad4pushVariables.LAT, StringType, true),
    StructField(Ad4pushVariables.LON, StringType, true),
    StructField(Ad4pushVariables.ALTITUDE, StringType, true),
    StructField(Ad4pushVariables.GEOLOCATION_CREATED, StringType, true),
    StructField(Ad4pushVariables.VERSIONSDK, StringType, true),
    StructField(Ad4pushVariables.FEEDBACK, StringType, true),
    StructField(Ad4pushVariables.TIME_ZONE, StringType, true),
    StructField(Ad4pushVariables.SYSTEM_OPTIN_NOTIFS, StringType, true),
    StructField(Ad4pushVariables.ENABLED_NOTIFS, StringType, true),
    StructField(Ad4pushVariables.ENABLED_INAPPS, StringType, true),
    StructField(Ad4pushVariables.RANDOMID, StringType, true),
    StructField(Ad4pushVariables.COUNTRYCODE, StringType, true),
    StructField(Ad4pushVariables.AGGREGATED_NUMBER_OF_PURCHASES, StringType, true),
    StructField(Ad4pushVariables.GENDER, StringType, true),
    StructField(Ad4pushVariables.HAS_SHARED_PRODUCT, StringType, true),
    StructField(Ad4pushVariables.LAST_ABANDONED_CART_DATE, StringType, true),
    StructField(Ad4pushVariables.LAST_ABANDONED_CART_PRODUCT, StringType, true),
    StructField(Ad4pushVariables.LASTORDERDATE, StringType, true),
    StructField(Ad4pushVariables.LAST_SEARCH, StringType, true),
    StructField(Ad4pushVariables.LAST_SEARCH_DATE, StringType, true),
    StructField(Ad4pushVariables.LEAD, StringType, true),
    StructField(Ad4pushVariables.LOGIN_USER_ID, StringType, true),
    StructField(Ad4pushVariables.MOST_VISITED_CATEGORY, StringType, true),
    StructField(Ad4pushVariables.ORDER_STATUS, StringType, true),
    StructField(Ad4pushVariables.PURCHASE, StringType, true),
    StructField(Ad4pushVariables.REGISTRATION, StringType, true),
    StructField(Ad4pushVariables.STATUS_IN_APP, StringType, true),
    StructField(Ad4pushVariables.WISHLIST_STATUS, StringType, true),
    StructField(Ad4pushVariables.WISHLIST_ADD, StringType, true),
    StructField(Ad4pushVariables.SHOP_COUNTRY, StringType, true),
    StructField(Ad4pushVariables.AMOUNT_BASKET, StringType, true),
    StructField(Ad4pushVariables.CART, StringType, true),
    StructField(Ad4pushVariables.SPECIFIC_CATEGORY_VISIT_COUNT, StringType, true),
    StructField(Ad4pushVariables.USER_NAME, StringType, true),
    StructField(Ad4pushVariables.LAST_VIEWED_CATEGORY, StringType, true),
    StructField(Ad4pushVariables.MAX_VISITED_CATEGORY, StringType, true),
    StructField(Ad4pushVariables.MOST_VISITED_COUNTS, StringType, true),
    StructField(Ad4pushVariables.IDFA, StringType, true),
    StructField(Ad4pushVariables.LAST_ORDER_DATE, StringType, true),
    StructField(Ad4pushVariables.SEARCH_DATE, StringType, true),
    StructField(Ad4pushVariables.WISHLIST_PRODUCTS_COUNT, StringType, true),
    StructField(Ad4pushVariables.RATED, StringType, true)
  ))

}
