package com.jabong.dap.model.order.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.UdfUtils
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 24/6/15.
 */
object SalesOrder {

  /**
   *
   * @param salesOrders
   * @return
   */
  def couponScore(salesOrders: DataFrame): DataFrame = {
    val salesOrderNew = salesOrders.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.COUPON_CODE).na.drop()
    val couponScore = salesOrderNew.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(count(SalesOrderVariables.COUPON_CODE) as SalesOrderVariables.COUPON_SCORE)
    return couponScore
  }

  def processVariables(salesOrderCalcFull: DataFrame, salesOrderIncr: DataFrame): DataFrame = {
    val salesOrderCalcIncr = salesOrderIncr.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(
      max(SalesOrderVariables.CREATED_AT) as ContactListMobileVars.LAST_ORDER_DATE,
      max(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.UPDATED_AT,
      min(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.FIRST_ORDER_DATE,
      count(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.ORDERS_COUNT,
      count(SalesOrderVariables.CREATED_AT) - count(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.DAYS_SINCE_LAST_ORDER
    )
    if (null == salesOrderCalcFull) {
      salesOrderCalcIncr
    } else {
      val joinedDF = salesOrderCalcFull.unionAll(salesOrderCalcIncr)
      val salesOrderCalcNewFull = joinedDF.groupBy(SalesOrderVariables.FK_CUSTOMER)
        .agg(
          max(ContactListMobileVars.LAST_ORDER_DATE) as ContactListMobileVars.LAST_ORDER_DATE,
          max(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.UPDATED_AT,
          min(SalesOrderVariables.FIRST_ORDER_DATE) as SalesOrderVariables.FIRST_ORDER_DATE,
          sum(SalesOrderVariables.ORDERS_COUNT) as SalesOrderVariables.ORDERS_COUNT,
          min(SalesOrderVariables.DAYS_SINCE_LAST_ORDER) + 1 as SalesOrderVariables.DAYS_SINCE_LAST_ORDER
        )
      salesOrderCalcNewFull
    }
  }

}
