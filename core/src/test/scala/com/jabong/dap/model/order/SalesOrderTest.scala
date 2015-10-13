package com.jabong.dap.model.order

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import com.jabong.dap.model.order.schema.OrderVarSchema
import com.jabong.dap.model.order.variables.SalesOrder
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 29/6/15.
 */
class SalesOrderTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _
  @transient var dfSalesOrder: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order1", OrderVarSchema.salesOrder)
    df2 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order2", OrderVarSchema.salesOrderCoupon)
    dfSalesOrder = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order", Schema.salesOrder)
  }

  "The result" should "have size 3" in {
    var ordersCount = SalesOrder.couponScore(df2)
    assert(ordersCount.collect.size == 3)
  }

}
