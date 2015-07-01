package com.jabong.dap.model.order

import com.jabong.dap.common.{MergeUtils, Spark, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec
import com.jabong.dap.model.order.variables.SalesOrder


/**
 * Created by jabong on 29/6/15.
 */
class SalesOrderTest extends FlatSpec with SharedSparkContext{

  @transient var sqlContext: SQLContext = _
  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = new SQLContext(Spark.getContext())

    df1 = sqlContext.read.json("test/sales_order1.json")
    df2 = sqlContext.read.json("test/sales_order2.json")
    df1.collect.foreach(println)
  }

  "The result Dataframe" should "have size 3" in {
    var ordersCount = SalesOrder.ordersPlaced(df1)
    ordersCount.collect.foreach(println)
    assert(ordersCount.collect.size == 3)
  }

  "The result" should "have size 3" in {
    var ordersCount = SalesOrder.couponScore(df2)
    ordersCount.collect.foreach(println)
    assert(ordersCount.collect.size == 3)
  }



}
