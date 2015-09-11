package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ CustomerVariables, SalesOrderItemVariables, SalesOrderVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 11/9/15.
 */
class SalesOrder extends LiveCustomerSelector with Logging {

  override def customerSelection(last30DaySalesOrderData: DataFrame, yesterdaySalesOrderItemData: DataFrame): DataFrame = {

    if (last30DaySalesOrderData == null || yesterdaySalesOrderItemData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val filterdSalesOrderItem = yesterdaySalesOrderItemData.filter(yesterdaySalesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) === 6)

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
