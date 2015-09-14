package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ CustomerVariables, SalesOrderItemVariables, SalesOrderVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 11/9/15.
 */
class ClosedOrder extends LiveCustomerSelector with Logging {

  val MAX_STATUS = "max_status"
  val MIN_STATUS = "min_status"
  val CLOSED = "_closed"

  override def customerSelection(last30DaySalesOrderData: DataFrame, yesterdaySalesOrderItemData: DataFrame): DataFrame = {

    if (last30DaySalesOrderData == null || yesterdaySalesOrderItemData == null) {

      logger.error("Data frame should not be null")

      return null

    }
    val dfClosedOrder = yesterdaySalesOrderItemData.filter(yesterdaySalesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) === OrderStatus.CLOSED_ORDER)
      .select(SalesOrderItemVariables.FK_SALES_ORDER, SalesOrderItemVariables.SKU)

    val groupedSalesOrderItem = yesterdaySalesOrderItemData.groupBy(SalesOrderItemVariables.FK_SALES_ORDER).agg(max(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) as MAX_STATUS,
      min(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) as MIN_STATUS
    ).select(
        col(SalesOrderItemVariables.FK_SALES_ORDER) as SalesOrderItemVariables.FK_SALES_ORDER + CLOSED,
        col(MAX_STATUS),
        col(MIN_STATUS)
      )
    val filterdSalesOrderItem = groupedSalesOrderItem.filter(groupedSalesOrderItem(MAX_STATUS) === OrderStatus.CLOSED_ORDER and groupedSalesOrderItem(MIN_STATUS) === OrderStatus.CLOSED_ORDER)

    val dfJoin = dfClosedOrder.join(filterdSalesOrderItem, filterdSalesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER + CLOSED) === dfClosedOrder(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)

    val dfResult = last30DaySalesOrderData.join(dfJoin, last30DaySalesOrderData(SalesOrderVariables.ID_SALES_ORDER) === dfJoin(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)
      .select(col(SalesOrderVariables.FK_CUSTOMER),
        col(SalesOrderVariables.CUSTOMER_EMAIL) as CustomerVariables.EMAIL,
        col(SalesOrderItemVariables.SKU)
      )

    return dfResult
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
