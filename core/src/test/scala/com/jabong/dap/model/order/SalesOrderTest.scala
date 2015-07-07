package com.jabong.dap.model.order

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{AppConfig, Config, SharedSparkContext}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.schema.OrderVarSchema
import com.jabong.dap.model.order.variables.SalesOrder
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by jabong on 29/6/15.
 */
class SalesOrderTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    val config = new Config(basePath = "basePath")
    AppConfig.config = config

    //    df1 = sqlContext.read.json("test/sales_order1.json")
    df1 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order1", OrderVarSchema.salesOrder)
    //    df2 = sqlContext.read.json("test/sales_order2.json")
    df2 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order2", OrderVarSchema.salesOrderCoupon)
    df1.collect.foreach(println)
  }

  "The result" should "have size 3" in {
    var ordersCount = SalesOrder.couponScore(df2)
    ordersCount.collect.foreach(println)
    assert(ordersCount.collect.size == 3)
  }

//  override def afterAll() {
//    super.afterAll()
//  }
}