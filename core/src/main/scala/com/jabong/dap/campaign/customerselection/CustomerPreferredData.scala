package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 15/9/15.
 */
class CustomerPreferredData extends CustomerSelector with Logging {

  override def customerSelection(lastSixDaysCustomerOrders: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame): DataFrame = {

    if (lastSixDaysCustomerOrders == null || last6thDaySalesOrderData == null || last6thDaySalesOrderItemData == null) {
      log("Data frame should not be null")
      return null
    }

    val successFulOrderItems = CampaignUtils.getSuccessfulOrders(last6thDaySalesOrderItemData)
      .select(
        SalesOrderItemVariables.FK_SALES_ORDER,
        SalesOrderItemVariables.SKU
      )

    val dfSalesOrder = last6thDaySalesOrderData.select(
      SalesOrderVariables.ID_SALES_ORDER,
      SalesOrderVariables.FK_CUSTOMER,
      SalesOrderVariables.CUSTOMER_EMAIL
    )

    //join SalesOrder and SalesOrderItem Data
    val dfJoinOrderAndItem = dfSalesOrder.join(successFulOrderItems, successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) === dfSalesOrder(SalesOrderVariables.ID_SALES_ORDER), SQL.INNER)
      .select(
        SalesOrderVariables.FK_CUSTOMER,
        SalesOrderItemVariables.SKU,
        SalesOrderVariables.CUSTOMER_EMAIL
      )

    val dfResult = lastSixDaysCustomerOrders.join(dfJoinOrderAndItem, lastSixDaysCustomerOrders(CustomerVariables.FK_CUSTOMER) === dfJoinOrderAndItem(SalesOrderVariables.FK_CUSTOMER), SQL.INNER)
      .select(
        lastSixDaysCustomerOrders(CustomerVariables.FK_CUSTOMER),
        dfJoinOrderAndItem(SalesOrderVariables.CUSTOMER_EMAIL) as CustomerVariables.EMAIL,
        lastSixDaysCustomerOrders(CustomerVariables.CITY),
        lastSixDaysCustomerOrders(ProductVariables.BRAND) as CustomerVariables.PREFERRED_BRAND,
        dfJoinOrderAndItem(SalesOrderItemVariables.SKU) as ProductVariables.SKU_SIMPLE
      )

    return dfResult
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
