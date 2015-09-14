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

  override def customerSelection(last30DaySalesOrderData: DataFrame, yesterdaySalesOrderItemData: DataFrame): DataFrame = {

    if (last30DaySalesOrderData == null || yesterdaySalesOrderItemData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val groupedSalesOrderItem = yesterdaySalesOrderItemData.groupBy(SalesOrderItemVariables.FK_SALES_ORDER).agg(first(SalesOrderItemVariables.SKU) as SalesOrderItemVariables.SKU,
      max(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) as MAX_STATUS,
      min(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) as MIN_STATUS
    )

    val filterdSalesOrderItem = groupedSalesOrderItem.filter(groupedSalesOrderItem("max_status") === OrderStatus.CLOSED_ORDER and groupedSalesOrderItem("min_status") === OrderStatus.CLOSED_ORDER)

    val dfJoin = last30DaySalesOrderData.join(filterdSalesOrderItem, last30DaySalesOrderData(SalesOrderVariables.ID_SALES_ORDER) === filterdSalesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)
      .select(col(SalesOrderVariables.FK_CUSTOMER),
        col(SalesOrderVariables.CUSTOMER_EMAIL) as CustomerVariables.EMAIL,
        col(SalesOrderItemVariables.SKU)
      )

    return dfJoin
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
