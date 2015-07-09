package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FlatSpec

/**
 * Created by jabong1145 on 7/7/15.
 */
class ReturnReTargetTest extends FlatSpec with SharedSparkContext {

  @transient var hiveContext: HiveContext = _
  @transient var testDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  var returnReTarget: ReturnReTarget = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = Spark.getHiveContext()
    returnReTarget = new ReturnReTarget()
    orderItemDataFrame = hiveContext.read.json("src/test/resources/sales_order/sales_order_with_item.json")
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }

  "empty order data " should "return empty data from execute function of return re-target " in {
    val skuData = returnReTarget.execute(null)
    assert(skuData == null)
  }

  "Last day order items data " should "return two records from execute function" in {
    val skuData = returnReTarget.execute(orderItemDataFrame)
    assert(skuData.count() == 1)
  }

}
