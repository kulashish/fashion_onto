package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.variables.{ CustomerVariables, SalesOrderItemVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Return cancel customer selector test cases
 */
class ReturnCancelTest extends FlatSpec with SharedSparkContext {

  //  @transient var sqlContext: SQLContext = _
  @transient var orderData: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  var returnCancel: ReturnCancel = _

  override def beforeAll() {
    super.beforeAll()
    //    sqlContext = Spark.getSqlContext()
    returnCancel = new ReturnCancel()
    orderItemDataFrame = JsonUtils.readFromJson("campaigns", "sales_item_cancel_return", Schema.salesOrderItem)
    orderData = JsonUtils.readFromJson("campaigns", "sales_order_cancel_return", Schema.salesOrder)
  }

  "No order data" should "return no data" in {
    val customerSelectedData = returnCancel.customerSelection(null, orderItemDataFrame)
    assert(customerSelectedData == null)
  }

  "No order item data" should "return no data" in {
    val customerSelectedData = returnCancel.customerSelection(orderData, null)
    assert(customerSelectedData == null)
  }

  "Last days order data of customer id 16646865" should "return item price 1213" in {
    val customerSelectedData = returnCancel.customerSelection(orderData, orderItemDataFrame)
    val unitPrice = customerSelectedData.filter(CustomerVariables.FK_CUSTOMER + "=16646865")
      .select(SalesOrderItemVariables.UNIT_PRICE).collect()(0)(0).asInstanceOf[java.math.BigDecimal].doubleValue()
    assert(unitPrice == 1213.0)
  }

  "Last days order data of customer id 2898599" should "will get filtered because order has been placed after the item has been cancelled" in {
    val customerSelectedData = returnCancel.customerSelection(orderData, orderItemDataFrame)
    val value = customerSelectedData.filter(CustomerVariables.FK_CUSTOMER + "=2898599")
    assert(value.count() == 2)
  }
}
