package com.jabong.dap.model.ad4push.schema

import com.jabong.dap.common.constants.variables.DevicesReactionsVariables
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
    StructField(DevicesReactionsVariables.CLICK_7, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_15, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_30, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_LIFETIME, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_MONDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_TUESDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_WEDNESDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_THURSDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_FRIDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_SATURDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICK_SUNDAY, IntegerType, true),
    StructField(DevicesReactionsVariables.CLICKED_TWICE, IntegerType, true),
    StructField(DevicesReactionsVariables.MOST_CLICK_DAY, StringType, true)
  ))
}
