package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.GroupedUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.data.storage.schema.Schema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ TimestampType, DecimalType }

/**
 * Created by raghu on 29/9/15.
 */
class LastOrder extends LiveCustomerSelector with Logging {

  override def customerSelection(salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {

    if (salesOrder == null || salesOrderItem == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val groupedFields = Array(SalesOrderVariables.FK_CUSTOMER)
    val aggFields = Array(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CUSTOMER_EMAIL, SalesOrderVariables.ID_SALES_ORDER)
    val groupedSalesOrder = GroupedUtils.orderGroupBy(salesOrder, groupedFields, aggFields, GroupedUtils.LAST, Schema.lastOrder, SalesOrderVariables.CREATED_AT, GroupedUtils.ASC, TimestampType)

    //    val groupedSalesOrder = salesOrder.sort(SalesOrderVariables.CREATED_AT)
    //      .groupBy(SalesOrderVariables.FK_CUSTOMER)
    //      .agg(last(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.ID_SALES_ORDER)

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
