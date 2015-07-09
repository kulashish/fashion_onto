package com.jabong.dap.model.order.schema

import com.jabong.dap.common.constants.variables.{ SalesAddressVariables, SalesOrderVariables, SalesRuleVariables }
import org.apache.spark.sql.types._

/**
 * Created by pooja on 7/7/15.
 */
object OrderVarSchema {

  val salesRule = StructType(Array(
    StructField(SalesRuleVariables.FK_CUSTOMER, IntegerType, true),
    StructField(SalesRuleVariables.UPDATED_AT, TimestampType, true),
    StructField(SalesRuleVariables.CODE, StringType, true),
    StructField(SalesRuleVariables.CREATED_AT, TimestampType, true),
    StructField(SalesRuleVariables.TO_DATE, TimestampType, true))
  )

  val salesOrder = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, IntegerType, true),
    StructField(SalesOrderVariables.CREATED_AT, TimestampType, true))
  )

  val salesOrderAddress = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, IntegerType, true),
    StructField(SalesAddressVariables.CITY, StringType, true),
    StructField(SalesAddressVariables.PHONE, StringType, true))
  )

  val salesOrderCoupon = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, IntegerType, true),
    StructField(SalesOrderVariables.COUPON_CODE, StringType, true))
  )
}
