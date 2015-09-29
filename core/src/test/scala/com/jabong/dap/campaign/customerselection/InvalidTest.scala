package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by rahul for invalid customer selection test cases on 14/7/15.
 */
class InvalidTest extends FlatSpec with SharedSparkContext {
  // @transient var sqlContext: SQLContext = _
  @transient var salesOrderItemDataFrame: DataFrame = _
  @transient var orderDataFrame: DataFrame = _
  @transient var salesOrderItemDataFrame1: DataFrame = _
  @transient var orderDataFrame1: DataFrame = _
  var invalidCustomerSelection: Invalid = _

  override def beforeAll() {
    super.beforeAll()
    // sqlContext = Spark.getSqlContext()
    invalidCustomerSelection = new Invalid()
    salesOrderItemDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "sales_item_invalid")
    orderDataFrame = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "sales_order_invalid")
    salesOrderItemDataFrame1 = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "sales_item_invalid1")
    orderDataFrame1 = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "sales_order_invalid1")
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
    assert(expectedOrders.count() == 9)
  }

  "One days orders data" should "return null invalid orders" in {
    val expectedOrders = invalidCustomerSelection.getInvalidOrders(salesOrderItemDataFrame)
    assert(expectedOrders.count() == 4)
  }

  "No Orders data" should "return null customer selection" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(null, salesOrderItemDataFrame)
    assert(expectedOrders == null)
  }

  "No Orders Items data" should "return null customer selection" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame, null)
    assert(expectedOrders == null)
  }
  //
  //  "Negative days as parameter" should "return null customer selection" in {
  //    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame, salesOrderItemDataFrame, -30)
  //    assert(expectedOrders == null)
  //  }

  "30 days salesOrder data with orders data" should "return one customer with invalid order" in {
    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame, salesOrderItemDataFrame)
    assert(expectedOrders.count() == 1)
  }

  "salesOrder data with with updated at of invalid order is less than successful order" should "return null customer selection  " in {
    val expectedOrders = invalidCustomerSelection.customerSelection(orderDataFrame1, salesOrderItemDataFrame1)
    assert(expectedOrders.count() == 0)
  }

}
