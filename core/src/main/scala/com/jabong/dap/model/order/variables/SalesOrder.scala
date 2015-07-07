package com.jabong.dap.model.order.variables

import com.jabong.dap.common.constants.variables.SalesOrderVariables
import org.apache.spark.sql.DataFrame
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

  def processVariables(prev: DataFrame, curr: DataFrame): DataFrame = {
    val gRDD = curr.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(max(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.LAST_ORDER_DATE,
      min(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.FIRST_ORDER_DATE,
      count(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.ORDERS_COUNT,
      count(SalesOrderVariables.CREATED_AT) - count(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.DAYS_SINCE_LAST_ORDER)
    val joinedRDD = prev.unionAll(gRDD)
    val res = joinedRDD.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(max(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.LAST_ORDER_DATE,
      min(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.FIRST_ORDER_DATE,
      sum(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.ORDERS_COUNT,
      min(SalesOrderVariables.DAYS_SINCE_LAST_ORDER) + 1 as SalesOrderVariables.DAYS_SINCE_LAST_ORDER)

    return res
  }

}
