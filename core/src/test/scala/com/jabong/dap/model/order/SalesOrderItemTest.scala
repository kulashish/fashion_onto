package com.jabong.dap.model.order

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.schema.OrderVarSchema
import com.jabong.dap.model.order.variables.SalesOrderItem
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 29/6/15.
 */
class SalesOrderItemTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _
  @transient var df3: DataFrame = _
  @transient var df4: DataFrame = _
  @transient var df5: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_app")

    df2 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_web")

    df3 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_mweb")

    df4 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item1", OrderVarSchema.salesOrderItem)

    df5 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order")

  }

  "Testing JoinDataframe" should "have size 148" in {
    var ordersCount = SalesOrderItem.joinDataFrames(df1, df2, df3)
    assert(ordersCount.collect.size == 148)
  }

//  "Testing Successful orders Count" should "have size 3" in {
//    var ordersCount = SalesOrderItem.getSucessfullOrders(df5, df4)
//    assert(ordersCount.collect.size == 3)
//  }

}