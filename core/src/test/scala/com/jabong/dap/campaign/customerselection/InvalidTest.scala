package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.{ Spark, SharedSparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Created by rahul for invalid customer selection test cases on 14/7/15.
 */
class InvalidTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var salesOrderItemDataFrame: DataFrame = _
  @transient var orderDataFrame: DataFrame = _
  var invalidCustomerSelection: Invalid = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    invalidCustomerSelection = new Invalid()
    salesOrderItemDataFrame = sqlContext.read.json("src/test/resources/campaign/sales_item_invalid.json")
    orderDataFrame = sqlContext.read.json("src/test/resources/campaign/sales_order_invalid.json")
  }

  "No orders data" should "return null successful orders" in {
    val expectedOrders = invalidCustomerSelection.getSuccessfulOrders(null)
    assert(expectedOrders == null)
  }

  "No orders data" should "return null invalid orders" in {
    val expectedOrders = invalidCustomerSelection.getInvalidOrders(null)
    assert(expectedOrders == null)
  }

  "One days orders data" should "return null successful orders" in {
    val expectedOrders = invalidCustomerSelection.getSuccessfulOrders(salesOrderItemDataFrame)
    assert(expectedOrders.count() == 3)
  }

  "One days orders data" should "return null invalid orders" in {
    val expectedOrders = invalidCustomerSelection.getInvalidOrders(salesOrderItemDataFrame)
    assert(expectedOrders.count() == 4)
  }

  "No Orders data" should "return null customer selection" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(null, salesOrderItemDataFrame, 30)
    assert(expectedOrders == null)
  }

  "No Orders Items data" should "return null customer selection" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame, null, 30)
    assert(expectedOrders == null)
  }

  "Negative days as parameter" should "return null customer selection" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame, salesOrderItemDataFrame, -30)
    assert(expectedOrders == null)
  }

  "30 days salesOrder data with orders data" should "return null customer selection" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame, salesOrderItemDataFrame, 30)
    assert(expectedOrders.count() == 1)
  }

}
