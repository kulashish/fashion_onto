package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 15/9/15.
 */
class CustomerPreferredData extends CustomerSelector with Logging {

  override def customerSelection(yestCustomerData: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame): DataFrame = {

    if (yestCustomerData == null || last6thDaySalesOrderData == null || last6thDaySalesOrderItemData == null) {
      log("Data frame should not be null")
      return null
    }

    val successFulOrderItems = CampaignUtils.getSuccessfulOrders(last6thDaySalesOrderItemData)

    //join SalesOrder and SalesOrderItem Data
    val dfJoinOrderAndItem = last6thDaySalesOrderData.join(successFulOrderItems, successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) === last6thDaySalesOrderData(SalesOrderVariables.ID_SALES_ORDER), SQL.INNER)
      .select(SalesOrderVariables.FK_CUSTOMER, SalesOrderItemVariables.SKU)

    val dfCustomerData = yestCustomerData.select(CustomerVariables.ID_CUSTOMER, CustomerVariables.CITY, ProductVariables.BRAND)

    val dfResult = dfCustomerData.join(dfJoinOrderAndItem, dfCustomerData(CustomerVariables.ID_CUSTOMER) === dfJoinOrderAndItem(SalesOrderVariables.FK_CUSTOMER), SQL.INNER)
      .select(CustomerVariables.ID_CUSTOMER, CustomerVariables.CITY, ProductVariables.BRAND, SalesOrderItemVariables.SKU)

    return dfResult
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
