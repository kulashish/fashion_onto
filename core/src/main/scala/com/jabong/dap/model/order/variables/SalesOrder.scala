package com.jabong.dap.model.order.variables

import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, SalesOrderVariables }
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
    couponScore
  }

  def getlastOrderDate(salesOrderIncr: DataFrame): DataFrame = {
    salesOrderIncr.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(
      max(SalesOrderVariables.CREATED_AT) as ContactListMobileVars.LAST_ORDER_DATE,
      max(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.UPDATED_AT
    )
  }

}
