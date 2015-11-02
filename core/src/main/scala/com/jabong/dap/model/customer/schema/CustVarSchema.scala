package com.jabong.dap.model.customer.schema

import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, ContactListMobileVars, CustomerVariables }
import org.apache.spark.sql.types._

/**
 * Created by raghu on 2/7/15.
 */
object CustVarSchema {
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val customersPreferredOrderTimeslotPart2 = StructType(Array(
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.ORDER_0, IntegerType, true),
    StructField(CustomerVariables.ORDER_1, IntegerType, true),
    StructField(CustomerVariables.ORDER_2, IntegerType, true),
    StructField(CustomerVariables.ORDER_3, IntegerType, true),
    StructField(CustomerVariables.ORDER_4, IntegerType, true),
    StructField(CustomerVariables.ORDER_5, IntegerType, true),
    StructField(CustomerVariables.ORDER_6, IntegerType, true),
    StructField(CustomerVariables.ORDER_7, IntegerType, true),
    StructField(CustomerVariables.ORDER_8, IntegerType, true),
    StructField(CustomerVariables.ORDER_9, IntegerType, true),
    StructField(CustomerVariables.ORDER_10, IntegerType, true),
    StructField(CustomerVariables.ORDER_11, IntegerType, true),
    StructField(CustomerVariables.PREFERRED_ORDER_TIMESLOT, IntegerType, true)
  ))

  val customersPreferredOrderTimeslotPart1 = StructType(Array(
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.OPEN_0, IntegerType, true),
    StructField(CustomerVariables.OPEN_1, IntegerType, true),
    StructField(CustomerVariables.OPEN_2, IntegerType, true),
    StructField(CustomerVariables.OPEN_3, IntegerType, true),
    StructField(CustomerVariables.OPEN_4, IntegerType, true),
    StructField(CustomerVariables.OPEN_5, IntegerType, true),
    StructField(CustomerVariables.OPEN_6, IntegerType, true),
    StructField(CustomerVariables.OPEN_7, IntegerType, true),
    StructField(CustomerVariables.OPEN_8, IntegerType, true),
    StructField(CustomerVariables.OPEN_9, IntegerType, true),
    StructField(CustomerVariables.OPEN_10, IntegerType, true),
    StructField(CustomerVariables.OPEN_11, IntegerType, true),
    StructField(CustomerVariables.CLICK_0, IntegerType, true),
    StructField(CustomerVariables.CLICK_1, IntegerType, true),
    StructField(CustomerVariables.CLICK_2, IntegerType, true),
    StructField(CustomerVariables.CLICK_3, IntegerType, true),
    StructField(CustomerVariables.CLICK_4, IntegerType, true),
    StructField(CustomerVariables.CLICK_5, IntegerType, true),
    StructField(CustomerVariables.CLICK_6, IntegerType, true),
    StructField(CustomerVariables.CLICK_7, IntegerType, true),
    StructField(CustomerVariables.CLICK_8, IntegerType, true),
    StructField(CustomerVariables.CLICK_9, IntegerType, true),
    StructField(CustomerVariables.CLICK_10, IntegerType, true),
    StructField(CustomerVariables.CLICK_11, IntegerType, true),
    StructField(CustomerVariables.PREFERRED_OPEN_TIMESLOT, IntegerType, true),
    StructField(CustomerVariables.PREFERRED_CLICK_TIMESLOT, IntegerType, true)
  ))

  val emailOpen = StructType(Array(
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.OPEN_0, IntegerType, true),
    StructField(CustomerVariables.OPEN_1, IntegerType, true),
    StructField(CustomerVariables.OPEN_2, IntegerType, true),
    StructField(CustomerVariables.OPEN_3, IntegerType, true),
    StructField(CustomerVariables.OPEN_4, IntegerType, true),
    StructField(CustomerVariables.OPEN_5, IntegerType, true),
    StructField(CustomerVariables.OPEN_6, IntegerType, true),
    StructField(CustomerVariables.OPEN_7, IntegerType, true),
    StructField(CustomerVariables.OPEN_8, IntegerType, true),
    StructField(CustomerVariables.OPEN_9, IntegerType, true),
    StructField(CustomerVariables.OPEN_10, IntegerType, true),
    StructField(CustomerVariables.OPEN_11, IntegerType, true),
    StructField(CustomerVariables.PREFERRED_OPEN_TIMESLOT, IntegerType, true)
  ))

  val emailClick = StructType(Array(
    StructField(CustomerVariables.CUSTOMER_ID, StringType, true),
    StructField(CustomerVariables.CLICK_0, IntegerType, true),
    StructField(CustomerVariables.CLICK_1, IntegerType, true),
    StructField(CustomerVariables.CLICK_2, IntegerType, true),
    StructField(CustomerVariables.CLICK_3, IntegerType, true),
    StructField(CustomerVariables.CLICK_4, IntegerType, true),
    StructField(CustomerVariables.CLICK_5, IntegerType, true),
    StructField(CustomerVariables.CLICK_6, IntegerType, true),
    StructField(CustomerVariables.CLICK_7, IntegerType, true),
    StructField(CustomerVariables.CLICK_8, IntegerType, true),
    StructField(CustomerVariables.CLICK_9, IntegerType, true),
    StructField(CustomerVariables.CLICK_10, IntegerType, true),
    StructField(CustomerVariables.CLICK_11, IntegerType, true),
    StructField(CustomerVariables.PREFERRED_CLICK_TIMESLOT, IntegerType, true)
  ))
  val resultCustomer = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER, LongType, true),
    StructField(CustomerVariables.GIFTCARD_CREDITS_AVAILABLE, DecimalType(10, 2), true),
    StructField(CustomerVariables.STORE_CREDITS_AVAILABLE, DecimalType(10, 2), true),
    StructField(CustomerVariables.BIRTHDAY, DateType, true),
    StructField(CustomerVariables.GENDER, StringType, true),
    StructField(CustomerVariables.REWARD_TYPE, StringType, true),
    StructField(CustomerVariables.EMAIL, StringType, true),
    StructField(CustomerVariables.CREATED_AT, TimestampType, true),
    StructField(CustomerVariables.UPDATED_AT, TimestampType, true),
    StructField(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT, StringType, true),
    StructField(CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT, IntegerType, true),
    StructField(CustomerVariables.FIRST_NAME, StringType, true),
    StructField(CustomerVariables.LAST_NAME, StringType, true),
    StructField(CustomerVariables.PHONE, StringType, true),
    StructField(CustomerVariables.CITY, StringType, true),
    StructField(ContactListMobileVars.VERIFICATION_STATUS, BooleanType, true),
    StructField(ContactListMobileVars.NL_SUB_DATE, TimestampType, true),
    StructField(ContactListMobileVars.UNSUB_KEY, StringType, true),
    StructField(ContactListMobileVars.AGE, IntegerType, true),
    StructField(ContactListMobileVars.REG_DATE, TimestampType, true),
    StructField(CustomerVariables.LAST_UPDATED_AT, TimestampType, true),
    StructField(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS, StringType, true)
  ))

}
