package com.jabong.dap.campaign.customerselection

import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
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

  // val sqlContext = Spark.getSqlContext()
  override def customerSelection(customerData: DataFrame): DataFrame = {
    return null
  }

  //  import sqlContext.implicits._

  /**
   * Sample usecase:
   *  1. return retargeting: find users who have initiated return request for their order yesterday
   *                         + no new order after that
   *
   *  Returns join of sales_order and sales_order_item data
   *    (id_customer, id_sales_order, item_status, unit_price, updated_at, sku_simple)
   *
   * @param customerOrderData - need to be full (optimize: last 30 day)
   * @param salesOrderItemData - one day incremental data of sales order item table
   * @return
   */

  def customerSelection(customerOrderData: DataFrame, salesOrderItemData: DataFrame): DataFrame = {
    if (customerOrderData == null || salesOrderItemData == null) {
      return null
    }

    // 1. order_item (1 day): filter by required status

    val returnCancelSku = salesOrderItemData.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " in (" + OrderStatus.RETURN
      + "," + OrderStatus.RETURN_PAYMENT_PENDING + "," + OrderStatus.CANCELLED + "," + OrderStatus.CANCELLED_CC_ITEM + "," + OrderStatus.CANCEL_PAYMENT_ERROR
      + "," + OrderStatus.DECLINED + "," + OrderStatus.EXPORTABLE_CANCEL_CUST + "," + OrderStatus.EXPORTED_CANCEL_CUST + ")")
      .select(salesOrderItemData(ProductVariables.SKU),
        salesOrderItemData(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        salesOrderItemData(SalesOrderItemVariables.UNIT_PRICE),
        salesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER),
        salesOrderItemData(SalesOrderItemVariables.UPDATED_AT))

    // 2. inner join it with sales_order: short data
    // call it customerLatestItemsData
    val customerLatestItemsData = customerOrderData.join(
      returnCancelSku,
      customerOrderData(SalesOrderVariables.ID_SALES_ORDER).equalTo(returnCancelSku(SalesOrderItemVariables.FK_SALES_ORDER)), SQL.INNER
    )
      .select(customerOrderData(SalesOrderVariables.FK_CUSTOMER),
        customerOrderData(SalesOrderVariables.ID_SALES_ORDER),
        returnCancelSku(ProductVariables.SKU),
        returnCancelSku(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        returnCancelSku(SalesOrderItemVariables.UNIT_PRICE),
        returnCancelSku(SalesOrderItemVariables.UPDATED_AT))

    // how to filter new orders after that
    // 1. sales_order subset: last day order created at, customers in the customerLatestItemsData
    // 2. on that, group on fk_customer, order by created_by and create (customer, last_sales_order_id, last_order_time)

    val yesterdayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT_MS))
    val yesterdayOldStartTime = TimeUtils.getStartTimestampMS(yesterdayOldTime)
    val yesterdayOldEndTime = TimeUtils.getEndTimestampMS(yesterdayOldTime)

    val yesterdayCustomerOrderData = CampaignUtils.getTimeBasedDataFrame(customerOrderData, SalesOrderVariables.CREATED_AT, yesterdayOldStartTime.toString, yesterdayOldEndTime.toString)

    val latestCustomerOrders = yesterdayCustomerOrderData
      // .orderBy($"${SalesOrderVariables.CREATED_AT}".desc).groupBy(SalesOrderVariables.FK_CUSTOMER).agg($"${SalesOrderVariables.FK_CUSTOMER}",
      .orderBy(desc(SalesOrderVariables.CREATED_AT)).groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(first(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.FK_SALES_ORDER,
        first(SalesOrderVariables.CREATED_AT) as "last_order_time")

    // 3. join it with rdf data and then filter by where item_updated_at < last_order_time
    val joinedData = customerLatestItemsData.join(latestCustomerOrders, latestCustomerOrders(SalesOrderVariables.FK_CUSTOMER) === customerLatestItemsData(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
    //or "+customerLatestItemsData(SalesOrderItemVariables.UPDATED_AT)<latestCustomerOrders( "last_order_time")
    val filteredSku = joinedData.filter("updated_at is not null and (last_order_time  is null or last_order_time <= updated_at)")
      .select(customerLatestItemsData(SalesOrderVariables.FK_CUSTOMER),
        customerLatestItemsData(SalesOrderVariables.ID_SALES_ORDER),
        customerLatestItemsData(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        customerLatestItemsData(SalesOrderItemVariables.UNIT_PRICE),
        customerLatestItemsData(SalesOrderItemVariables.UPDATED_AT),
        customerLatestItemsData(ProductVariables.SKU))

    //  println("FOURTH"+latestCustomerOrders.count())
    //    filteredSku.collect().foreach(println)

    return filteredSku

  }

  /**
   *
   * @param inData
   * @param ndays
   * @return
   */
  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
