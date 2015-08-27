package com.jabong.dap.common

import com.jabong.dap.common.constants.variables._
import org.apache.spark.sql.types._

/**
 * Created by pooja on 27/8/15.
 */
object TestSchema {

  val resultFullSkuFilter = StructType(Array(
    StructField(CustomerProductShortlistVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerProductShortlistVariables.EMAIL, StringType, true),
    StructField(ProductVariables.SKU_SIMPLE, StringType, true),
    StructField(ProductVariables.SPECIAL_PRICE, DecimalType(10, 2), true)
  ))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_segments variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val mvp_seg = StructType(Array(
    StructField(CustomerSegmentsVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerSegmentsVariables.MVP_TYPE, IntegerType, true),
    StructField(CustomerSegmentsVariables.SEGMENT, IntegerType, true),
    StructField(CustomerSegmentsVariables.DISCOUNT_SCORE, IntegerType, true)
  ))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_storecredits_history variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val last_jr_covert_date = StructType(Array(
    StructField(CustomerStoreVariables.FK_CUSTOMER, LongType, true),
    StructField(CustomerStoreVariables.LAST_JR_COVERT_DATE, TimestampType, true)
  ))

  val accRegDateAndUpdatedAt = StructType(Array(
    StructField(CustomerVariables.EMAIL, StringType, true),
    StructField(CustomerVariables.REG_DATE, TimestampType, true),
    StructField(CustomerVariables.UPDATED_AT, TimestampType, true)
  ))

  val emailOptInStatus = StructType(Array(
    StructField(CustomerVariables.ID_CUSTOMER, LongType, true),
    StructField(NewsletterVariables.STATUS, StringType, true)
  ))

}
