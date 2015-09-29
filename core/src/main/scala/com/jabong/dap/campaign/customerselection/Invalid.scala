package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * All the customers who have invalid order for last n days
 * Created by rahul for com.jabong.dap.campaign.customerselection on 13/7/15.
 */
class Invalid extends LiveCustomerSelector with Logging {
  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, nDays: Int): DataFrame = ???

  /**
   * return customers who have placed invalid order and haven't placed successful order for the same sku yet
   *
   *
   * @param customerOrderData - full
   * @param salesOrderItemData - filtered beforehand for n days
   * @return
   */
  override def customerSelection(customerOrderData: DataFrame, salesOrderItemData: DataFrame): DataFrame = {
    if (customerOrderData == null || salesOrderItemData == null) {
      logger.error("either of provided data is null")
      return null
    }
    // FIXME: need to for last day 0 to 24
    // FIXME: filter need to also have <=
    // val daysAfter = TimeUtils.getDateAfterNDays(-ndays,TimeConstants.DATE_TIME_FORMAT)
    //val lastDaysSalesItemData = salesOrderItemData.filter(SalesOrderItemVariables.UPDATED_AT + " >= '" + daysAfter+"'")

    //val lastDaysSalesItemData = salesOrderItemData

    // get Invalid Orders of last days sales item
    val inValidSku = getInvalidOrders(salesOrderItemData)

    // get successful Orders of last days sales item
    val successfulSku = getSuccessfulOrders(salesOrderItemData)

    // 2. inner join it with sales_order: short data
    // Now we have customers with invalid orders in last n days
    var customerInValidItemsData = customerOrderData.join(inValidSku,
      customerOrderData(SalesOrderVariables.ID_SALES_ORDER).equalTo(inValidSku(SalesOrderItemVariables.FK_SALES_ORDER)), SQL.INNER)
      .select(customerOrderData(SalesOrderVariables.FK_CUSTOMER),
        customerOrderData(SalesOrderVariables.ID_SALES_ORDER),
        inValidSku(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        inValidSku(SalesOrderItemVariables.UNIT_PRICE),
        inValidSku(SalesOrderItemVariables.UPDATED_AT),
        inValidSku(ProductVariables.SKU),
        inValidSku(SalesOrderItemVariables.FK_SALES_ORDER))

    val customerInValidItemsSchema = customerInValidItemsData.schema

    //rename customerInValidItemsData column names with invalid_ as prefix
    customerInValidItemsSchema.foreach(x => customerInValidItemsData = customerInValidItemsData.withColumnRenamed(x.name, "invalid_" + x.name))

    // get all successful customer orders
    // 2. inner join it with sales_order: short data
    // Now we have customers with successful orders in last n days
    var customerSuccessfulItemsData = customerOrderData.join(successfulSku,
      customerOrderData(SalesOrderVariables.ID_SALES_ORDER).equalTo(successfulSku(SalesOrderItemVariables.FK_SALES_ORDER)), SQL.INNER)
      .select(customerOrderData(SalesOrderVariables.FK_CUSTOMER), customerOrderData(SalesOrderVariables.ID_SALES_ORDER), successfulSku(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS), successfulSku(SalesOrderItemVariables.UNIT_PRICE), successfulSku(SalesOrderItemVariables.UPDATED_AT), successfulSku(ProductVariables.SKU), successfulSku(SalesOrderItemVariables.FK_SALES_ORDER))

    val customerSuccessfulItemsSchema = customerSuccessfulItemsData.schema

    //rename customerSuccessfulItemsData column names with success_ as prefix
    customerSuccessfulItemsSchema.foreach(x => customerSuccessfulItemsData = customerSuccessfulItemsData.withColumnRenamed(x.name, "success_" + x.name))

    // FIXME: change invalid names back to normal

    val customerSelected = customerInValidItemsData.join(customerSuccessfulItemsData, customerInValidItemsData("invalid_" + SalesOrderVariables.FK_CUSTOMER) === customerSuccessfulItemsData("success_" + SalesOrderVariables.FK_CUSTOMER)
      && customerInValidItemsData("invalid_" + ProductVariables.SKU) === customerSuccessfulItemsData("success_" + ProductVariables.SKU), SQL.LEFT_OUTER)
      .filter("success_" + SalesOrderItemVariables.FK_SALES_ORDER + " is null or invalid_" + SalesOrderItemVariables.UPDATED_AT + " > " + "success_" + SalesOrderItemVariables.UPDATED_AT)
      .select(
        customerInValidItemsData("invalid_" + SalesOrderVariables.FK_SALES_ORDER) as (SalesOrderVariables.FK_SALES_ORDER),
        customerInValidItemsData("invalid_" + CustomerVariables.FK_CUSTOMER) as (CustomerVariables.FK_CUSTOMER),
        customerInValidItemsData("invalid_" + ProductVariables.SKU) as (ProductVariables.SKU_SIMPLE),
        customerInValidItemsData("invalid_" + SalesOrderItemVariables.UNIT_PRICE) as (SalesOrderItemVariables.UNIT_PRICE),
        customerInValidItemsData("invalid_" + SalesOrderItemVariables.UPDATED_AT) as (SalesOrderItemVariables.CREATED_AT))

    return customerSelected
  }

  /**
   * get all Orders which are successful
   * @param salesOrderItemData
   * @return
   */
  def getSuccessfulOrders(salesOrderItemData: DataFrame): DataFrame = {
    if (salesOrderItemData == null) {
      return null
    }
    // Sales order skus with successful order status
    val successfulSku = salesOrderItemData.
      filter(SalesOrderItemVariables.FK_SALES_ORDER_ITEM + " != " + OrderStatus.CANCEL_PAYMENT_ERROR + " and " +
        SalesOrderItemVariables.FK_SALES_ORDER_ITEM + " != " + OrderStatus.INVALID)
      .select(salesOrderItemData(ProductVariables.SKU),
        salesOrderItemData(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        salesOrderItemData(SalesOrderItemVariables.UNIT_PRICE),
        salesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER),
        salesOrderItemData(SalesOrderItemVariables.UPDATED_AT))

    return successfulSku
  }

  /**
   * get all Invalid orders
   * @param salesOrderItemData
   * @return
   */
  def getInvalidOrders(salesOrderItemData: DataFrame): DataFrame = {
    if (salesOrderItemData == null) {
      return null
    }
    // Sales order skus with invalid status
    val inValidSku = salesOrderItemData.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " = " + OrderStatus.INVALID)
      .select(salesOrderItemData(ProductVariables.SKU), salesOrderItemData(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        salesOrderItemData(SalesOrderItemVariables.UNIT_PRICE), salesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER),
        salesOrderItemData(SalesOrderItemVariables.UPDATED_AT))

    return inValidSku
  }

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
