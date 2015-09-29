package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
 * Created by raghu on 29/9/15.
 */
class LastOrder extends LiveCustomerSelector with Logging {

  override def customerSelection(salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {

    if (salesOrder == null || salesOrderItem == null) {

      logger.error("Data frame should not be null")

      return null

    }
    val groupedSalesOrder = salesOrder.sort(SalesOrderVariables.CREATED_AT)
      .groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(last(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.ID_SALES_ORDER)

    val joinedDf = groupedSalesOrder.join(
      salesOrderItem,
      groupedSalesOrder(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER),
      SQL.INNER
    ).select(
        col(SalesOrderVariables.FK_CUSTOMER),
        col(SalesOrderVariables.CUSTOMER_EMAIL) as CustomerVariables.EMAIL,
        col(SalesOrderItemVariables.SKU) as ProductVariables.SKU_SIMPLE
      )

    joinedDf
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???

}
