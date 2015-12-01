package com.jabong.dap.model.order

import java.sql.Timestamp

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.TestSchema
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.variables.{ SalesItemRevenue, SalesOrderItem }
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

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_app")

    df2 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_web")

    df3 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_mweb")

    df4 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ITEM, "sales_order_item_joined", TestSchema.salesOrderItemJoined)

  }

  //  "Testing JoinDataframe" should "have size 148" in {
  //    var ordersCount = SalesItemRevenue.joinDataFrames(df1, df2, df3)
  //    assert(ordersCount.collect.size == 148)
  //  }

  "Testing makeMap4mGroupedData" should "have size 3" in {
    println(df4.count())
    val incrMap = df4.map(e =>
      (e(0).asInstanceOf[Long] -> (e(1).asInstanceOf[Long], e(2).asInstanceOf[Long], e(3).asInstanceOf[Int], e(4).asInstanceOf[Timestamp], e(5).asInstanceOf[Timestamp]))).groupByKey()
    val ordersMapIncr = incrMap.map(e => (e._1, SalesOrderItem.makeMap4mGroupedData(e._2.toList)))

    println("ordersMapIncr Count", ordersMapIncr.count())
    println(ordersMapIncr.toString())
  }

}