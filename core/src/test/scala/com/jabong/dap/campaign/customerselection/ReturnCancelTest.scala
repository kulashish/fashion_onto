package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, CustomerVariables }
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FlatSpec

/**
 * Return cancel customer selector test cases
 */
class ReturnCancelTest extends FlatSpec with SharedSparkContext {

  @transient var hiveContext: HiveContext = _
  @transient var orderData: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  var returnCancel: ReturnCancel = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = Spark.getHiveContext()
    returnCancel = new ReturnCancel()
    orderItemDataFrame = hiveContext.read.json("src/test/resources/sales_order/sales_order_item.json")
    orderData = hiveContext.read.json("src/test/resources/sales_order/sales_order_campaign.json")
  }

  "No order data" should "return no data" in {
    val customerSelectedData = returnCancel.customerSelection(null, orderItemDataFrame)
    assert(customerSelectedData == null)
  }

  "No order item data" should "return no data" in {
    val customerSelectedData = returnCancel.customerSelection(orderData, null)
    assert(customerSelectedData == null)
  }

  "Last days order data of customer id 2898599" should "return item price 499" in {
    val customerSelectedData = returnCancel.customerSelection(orderData, orderItemDataFrame)
    val unitPrice = customerSelectedData.filter(CustomerVariables.FK_CUSTOMER + "=2898599").select(SalesOrderItemVariables.UNIT_PRICE).collect()(0)(0).asInstanceOf[Double]
    assert(unitPrice == 499.0)
  }

}
