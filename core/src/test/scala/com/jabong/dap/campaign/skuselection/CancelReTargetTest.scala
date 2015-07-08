package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.{SharedSparkContext, Spark}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FlatSpec

/**
  Cancel ReTarget Test Class
 */
class CancelReTargetTest extends FlatSpec with SharedSparkContext {
  @transient var hiveContext: HiveContext = _
  @transient var testDataFrame : DataFrame = _
  @transient var orderItemDataFrame : DataFrame = _
  var cancelRetarget :CancelReTarget = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = Spark.getHiveContext()
    cancelRetarget = new CancelReTarget()
    orderItemDataFrame = hiveContext.read.json("src/test/resources/sales_order/sales_order_with_item.json")
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }


  "empty order data " should "return empty data from execute function" in {
    val skuData = cancelRetarget.execute(null)
    assert(skuData==null)
  }


  "Last day order items data " should "return empty data from execute function" in {
    val skuData = cancelRetarget.execute(orderItemDataFrame)
    assert(skuData.count()==2)
  }





}
