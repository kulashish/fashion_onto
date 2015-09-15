package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables, SalesOrderVariables, SalesOrderItemVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 15/9/15.
 */
object DCFBrandInCity {

  def skuFilter(yestCustomerData: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame): DataFrame = {

    if (yestCustomerData == null || last6thDaySalesOrderData == null || last6thDaySalesOrderItemData == null) {
      log("Data frame should not be null")
      return null
    }

    //join SalesOrder and SalesOrderItem Data
    val dfJoinOrderAndItem = last6thDaySalesOrderData.join(last6thDaySalesOrderItemData, last6thDaySalesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER) === last6thDaySalesOrderData(SalesOrderVariables.ID_SALES_ORDER), SQL.INNER)
      .select(SalesOrderVariables.FK_CUSTOMER, SalesOrderItemVariables.SKU)

    val dfCustomerData = yestCustomerData.select(CustomerVariables.ID_CUSTOMER, CustomerVariables.CITY, ProductVariables.BRAND)

    val dfResult = dfCustomerData.join(dfJoinOrderAndItem, dfCustomerData(CustomerVariables.ID_CUSTOMER) === dfJoinOrderAndItem(SalesOrderVariables.FK_CUSTOMER), SQL.INNER)
      .select(CustomerVariables.ID_CUSTOMER, CustomerVariables.CITY, ProductVariables.BRAND, SalesOrderItemVariables.SKU)

    return dfResult
  }

}
