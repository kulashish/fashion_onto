package com.jabong.dap.campaign.customerselect

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ProductVariables, SalesOrderItemVariables, SalesOrderVariables}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong1145 on 6/7/15.
 */
class ReturnCancel extends LiveCustomerSelection{

  val hiveContext = Spark.getHiveContext()
  override def customerSelection(customerData: DataFrame): DataFrame = {
    return null
  }
  import hiveContext.implicits._

  def customerSelection(customerOrderData: DataFrame,salesOrderItemData:DataFrame): DataFrame = {
    if(customerOrderData==null || salesOrderItemData==null) {
      return null
    }

    val latestCustomerOrders = customerOrderData.orderBy($"${SalesOrderVariables.UPDATED_AT}".desc).groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg($"${SalesOrderVariables.FK_CUSTOMER}",first(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.ID_SALES_ORDER
        ,first(SalesOrderVariables.UPDATED_AT) as "customer_updated_at")

    val customerLatestItemsData =  latestCustomerOrders.join(salesOrderItemData,
      latestCustomerOrders(SalesOrderVariables.ID_SALES_ORDER).equalTo(salesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER)),"inner")
      .select(SalesOrderVariables.FK_CUSTOMER,SalesOrderVariables.ID_SALES_ORDER
        ,ProductVariables.SKU,SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS,SalesOrderItemVariables.UNIT_PRICE)

//    customerLatestItemsData.collect().foreach(println)

    return customerLatestItemsData

  }


}
