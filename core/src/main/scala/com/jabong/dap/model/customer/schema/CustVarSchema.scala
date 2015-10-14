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
    StructField(SalesOrderVariables.CUSTOMER_ID, LongType, true),
    StructField(SalesOrderVariables.ORDER_0, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_1, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_2, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_3, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_4, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_5, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_6, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_7, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_8, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_9, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_10, IntegerType, true),
    StructField(SalesOrderVariables.ORDER_11, IntegerType, true),
    StructField(SalesOrderVariables.PREFERRED_ORDER_TIMESLOT, IntegerType, true)
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
