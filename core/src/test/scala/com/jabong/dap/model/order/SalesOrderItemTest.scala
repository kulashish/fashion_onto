package com.jabong.dap.model.order

import com.jabong.dap.common.{Spark, AppConfig, Config, SharedSparkContext}
import com.jabong.dap.data.storage.DataSets
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

  override def beforeAll() {
    super.beforeAll()

    val config = new Config(basePath = "basePath")
    AppConfig.config = config


    df1 = Spark.getSqlContext().read.parquet(DataSets.SALES_ORDER_ITEM+"/"+DataSets.SALES_ORDER_ITEM+"_app")

    df2 = Spark.getSqlContext().read.parquet(DataSets.SALES_ORDER_ITEM+"/"+DataSets.SALES_ORDER_ITEM+"_web")

    df3 = Spark.getSqlContext().read.parquet(DataSets.SALES_ORDER_ITEM+"/"+DataSets.SALES_ORDER_ITEM+"_mweb")

    df1.collect.foreach(println)
  }

  "Testing JoinDataframe" should "have size 148" in {
    var ordersCount = SalesOrderItem.joinDataFrames(df1,df2,df3)
    ordersCount.collect.foreach(println)
    assert(ordersCount.collect.size == 148)
  }


}