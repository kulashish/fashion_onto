package com.jabong.dap.model.order.schema

import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesAddressVariables, SalesOrderVariables, SalesRuleVariables }
import org.apache.spark.sql.types._

/**
 * Created by pooja on 7/7/15.
 */
object OrderVarSchema {

  val salesRule = StructType(Array(
    StructField(SalesRuleVariables.FK_CUSTOMER, LongType, true),
    StructField(SalesRuleVariables.UPDATED_AT, TimestampType, true),
    StructField(SalesRuleVariables.CODE, StringType, true),
    StructField(SalesRuleVariables.CREATED_AT, TimestampType, true),
    StructField(SalesRuleVariables.TO_DATE, TimestampType, true)
  ))

  val salesOrder = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
    StructField(SalesOrderVariables.CREATED_AT, TimestampType, true)
  ))

  val salesOrderAddress = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
    StructField(SalesAddressVariables.CITY, StringType, true),
    StructField(SalesAddressVariables.PHONE, StringType, true),
    StructField(SalesAddressVariables.FIRST_NAME, StringType, true),
    StructField(SalesAddressVariables.LAST_NAME, StringType, true)
  ))

  val salesOrderCoupon = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
    StructField(SalesOrderVariables.COUPON_CODE, StringType, true)
  ))

  val salesOrderItem = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER, IntegerType, true),
    StructField(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS, IntegerType, true)
  ))
}
