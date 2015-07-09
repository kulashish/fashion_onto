package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Returns a list of customers based on following conditions:
 *      1. Has a corresponding order-item who status was updated to "required set of status" yesterday
 *      2. No new order has been made by him/her after the corresponding order-item update
 *
 *  Returns data in the following format:
 *    - list of [(id_customer, id_sales_order, sku_simple, item status, unit price (special price))]
 *    - primary key is (id_customer, sku_simple), i.e., id_customer may bet repeated
 *
 */
class ReturnCancel extends LiveCustomerSelector {

  val hiveContext = Spark.getHiveContext()
  override def customerSelection(customerData: DataFrame): DataFrame = {
    return null
  }
  import hiveContext.implicits._

  /**
   * Sample usecase:
   *  1. return retargeting: find users who have initiated return request for their order yesterday
   *                         + no new order after that
   *
   *  Returns join of sales_order and sales_order_item data
   *
   * @param customerOrderData - need to be full (optimize: last 30 day)
   * @param salesOrderItemData - one day incremental data of sales order item table
   * @return
   */

  def customerSelection(customerOrderData: DataFrame, salesOrderItemData: DataFrame): DataFrame = {
    if (customerOrderData == null || salesOrderItemData == null) {
      return null
    }

    val latestCustomerOrders = customerOrderData.orderBy($"${SalesOrderVariables.UPDATED_AT}".desc).groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg($"${SalesOrderVariables.FK_CUSTOMER}", first(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.ID_SALES_ORDER, first(SalesOrderVariables.UPDATED_AT) as "customer_updated_at")

    // join sales_order and sales_order_item on 'id_sales_order' and select (id_customer, id_sales_order, sku (simple), item status, unit price)
    val customerLatestItemsData = latestCustomerOrders.join(salesOrderItemData,
      latestCustomerOrders(SalesOrderVariables.ID_SALES_ORDER).equalTo(salesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER)), "inner")
      .select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.ID_SALES_ORDER, ProductVariables.SKU, SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS, SalesOrderItemVariables.UNIT_PRICE)

    // 1. order_item (1 day): filter by required status
    // 2. inner join it with sales_order: short data
    // call it rdf

    // how to filter new orders after that
    // 1. sales_order subset: last day order created at, customers in the rdf
    // 2. on that, group on fk_customer, order by created_by and create (customer, last_order_time)
    // 3. join it with rdf data and then filter by where item_updated_at < last_order_time

    //    customerLatestItemsData.collect().foreach(println)

    return customerLatestItemsData

  }

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???
}
