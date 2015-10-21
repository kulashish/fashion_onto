package com.jabong.dap.model.customer.schema

import com.jabong.dap.common.constants.variables.EmailResponseVariables
import org.apache.spark.sql.types._

/**
 * Created by samatha on 13/10/15.
 */
object CustEmailSchema {
  val resCustomerEmail = StructType(Array(
    StructField(EmailResponseVariables.CUSTOMER_ID, StringType, true),
    StructField(EmailResponseVariables.UID, StringType, true),
    StructField(EmailResponseVariables.OPEN_SEGMENT, IntegerType, true),
    StructField(EmailResponseVariables.OPEN_7DAYS, IntegerType, true),
    StructField(EmailResponseVariables.OPEN_15DAYS, IntegerType, true),
    StructField(EmailResponseVariables.OPEN_30DAYS, IntegerType, true),
    StructField(EmailResponseVariables.CLICK_7DAYS, IntegerType, true),
    StructField(EmailResponseVariables.CLICK_15DAYS, IntegerType, true),
    StructField(EmailResponseVariables.CLICK_30DAYS, IntegerType, true),
    StructField(EmailResponseVariables.LAST_OPEN_DATE, DateType, true),
    StructField(EmailResponseVariables.LAST_CLICK_DATE, DateType, true),
    StructField(EmailResponseVariables.OPENS_LIFETIME, IntegerType, true),
    StructField(EmailResponseVariables.CLICKS_LIFETIME, IntegerType, true)
  ))

  val effectiveSchema = StructType(Array(
  StructField(EmailResponseVariables.CUSTOMER_ID, StringType, true),
    StructField(EmailResponseVariables.LAST_OPEN_DATE, StringType, true),
    StructField(EmailResponseVariables.LAST_CLICK_DATE, StringType, true),
    StructField(EmailResponseVariables.OPENS_LIFETIME, IntegerType, true),
    StructField(EmailResponseVariables.CLICKS_LIFETIME, IntegerType, true)
  ))
  val effective7daysSchema = StructType(Array(
    StructField(EmailResponseVariables.CUSTOMER_ID, StringType, true),
    StructField(EmailResponseVariables.LAST_OPEN_DATE, StringType, true),
    StructField(EmailResponseVariables.LAST_CLICK_DATE, StringType, true),
    StructField(EmailResponseVariables.OPENS_LIFETIME, IntegerType, true),
    StructField(EmailResponseVariables.CLICKS_LIFETIME, IntegerType, true),
    StructField(EmailResponseVariables.CLICK_7DAYS, IntegerType, true),
    StructField(EmailResponseVariables.OPEN_7DAYS, IntegerType, true)


  ))

}
