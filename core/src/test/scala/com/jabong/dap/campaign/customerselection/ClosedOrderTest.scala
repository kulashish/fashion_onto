package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 14/9/15.
 */
class ClosedOrderTest extends FlatSpec with SharedSparkContext {
  @transient var last30DaySalesOrderData: DataFrame = _
  @transient var yesterdaySalesOrderItemData: DataFrame = _
  var closedOrder: ClosedOrder = _

  override def beforeAll() {
    super.beforeAll()
    closedOrder = new ClosedOrder()
    last30DaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "sales_order")
    yesterdaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "sales_order_item")
  }

  "Null salesCartData DataFrame" should "return null" in {
    val customerSelected = closedOrder.customerSelection(null, null)
    assert(customerSelected == null)
  }

  "salesCartData DataFrame " should "return 2" in {
    val customerSelected = closedOrder.customerSelection(last30DaySalesOrderData, yesterdaySalesOrderItemData)
    println("count:" + customerSelected.count())
    assert(customerSelected.count() == 2)
  }

}