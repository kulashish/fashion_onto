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

  override def customerSelection(fullCustomerOrders: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame): DataFrame = {

    if (fullCustomerOrders == null || last6thDaySalesOrderData == null || last6thDaySalesOrderItemData == null) {
      log("Data frame should not be null")
      return null
    }

    val successFulOrderItems = CampaignUtils.getSuccessfulOrders(last6thDaySalesOrderItemData)

    val customerData = fullCustomerOrders.select(
      fullCustomerOrders(CustomerVariables.FK_CUSTOMER),
      fullCustomerOrders(SalesOrderItemVariables.FAV_BRAND) as ProductVariables.BRAND,
      fullCustomerOrders(CustomerVariables.CITY)
    )

    //join SalesOrder and SalesOrderItem Data
    val dfJoinOrderAndItem = last6thDaySalesOrderData.join(successFulOrderItems, successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) === last6thDaySalesOrderData(SalesOrderVariables.ID_SALES_ORDER), SQL.INNER)
      .select(
        SalesOrderVariables.FK_CUSTOMER,
        SalesOrderItemVariables.SKU,
        SalesOrderVariables.CUSTOMER_EMAIL
      )

    val dfResult = customerData.join(dfJoinOrderAndItem, customerData(CustomerVariables.FK_CUSTOMER) === dfJoinOrderAndItem(SalesOrderVariables.FK_CUSTOMER), SQL.INNER)
      .select(
        customerData(CustomerVariables.FK_CUSTOMER),
        dfJoinOrderAndItem(SalesOrderVariables.CUSTOMER_EMAIL) as CustomerVariables.EMAIL,
        customerData(CustomerVariables.CITY),
        customerData(ProductVariables.BRAND) as CustomerVariables.PREFERRED_BRAND,
        dfJoinOrderAndItem(SalesOrderItemVariables.SKU) as ProductVariables.SKU_SIMPLE
      )

    return dfResult
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
