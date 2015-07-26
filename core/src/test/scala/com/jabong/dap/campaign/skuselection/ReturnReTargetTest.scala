package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FlatSpec

/**
 * return reTarget sku selection test cases
 */
class ReturnReTargetTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var testDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  var returnReTarget: ReturnReTarget = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    returnReTarget = new ReturnReTarget()
    orderItemDataFrame = sqlContext.read.json("src/test/resources/sales_order/sales_order_with_item.json")
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }

  "empty order data " should "return empty data from execute function of return re-target " in {
    val skuData = returnReTarget.skuFilter(null)
    assert(skuData == null)
  }

  //FIXME: change the test cases to pass
    "Last day order items data " should "return two records from execute function" in {
      val skuData = returnReTarget.skuFilter(orderItemDataFrame)
      assert(skuData.count() == 1)
    }

}
