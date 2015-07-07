package com.jabong.dap.model.customer.schema

import com.jabong.dap.common.constants.variables.{ CustomerSegmentsVariables, CustomerStoreVariables, CustomerVariables, NewsletterVariables }
import org.apache.spark.sql.types._

/**
 * Created by raghu on 2/7/15.
 */
object CustVarSchema {
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val emailOptInStatus = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER, IntegerType, true),
    StructField(NewsletterVariables.STATUS, StringType, true)))

  val accRegDateAndUpdatedAt = StructType(Array(
    StructField(CustomerVariables.EMAIL, StringType, true),
    StructField(CustomerVariables.ACC_REG_DATE, TimestampType, true),
    StructField(CustomerVariables.UPDATED_AT, TimestampType, true)))

  val customersPreferredOrderTimeslot = StructType(Array(
    StructField(CustomerVariables.FK_CUSTOMER_CPOT, IntegerType, true),
    StructField(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT, StringType, true),
    StructField(CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT, IntegerType, true)))

  val resultCustomer = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER, IntegerType, true),
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
    StructField(CustomerVariables.ACC_REG_DATE, TimestampType, true),
    StructField(CustomerVariables.MAX_UPDATED_AT, TimestampType, true),
    StructField(CustomerVariables.EMAIL_OPT_IN_STATUS, StringType, true)))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_segments variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val mvp_seg = StructType(Array(
    StructField(CustomerSegmentsVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerSegmentsVariables.MVP_SCORE, IntegerType, true),
    StructField(CustomerSegmentsVariables.SEGMENT, IntegerType, true)))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_storecredits_history variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val last_jr_covert_date = StructType(Array(
    StructField(CustomerStoreVariables.FK_CUSTOMER, IntegerType, true),
    StructField(CustomerStoreVariables.LAST_JR_COVERT_DATE, TimestampType, true)))

}
