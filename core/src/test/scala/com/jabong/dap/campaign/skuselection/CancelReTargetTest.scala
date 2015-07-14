package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.customerselection.ReturnCancel
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.FlatSpec

/**
 * Cancel ReTarget Test Class
 */
class CancelReTargetTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var orderData: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _
  @transient var orderItemDataFrame1: DataFrame = _
  var cancelRetarget: CancelReTarget = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getHiveContext()
    cancelRetarget = new CancelReTarget()

    orderItemDataFrame = sqlContext.read.json("src/test/resources/sales_order/sales_order_with_item.json")
    orderItemDataFrame1 = sqlContext.read.json("src/test/resources/campaign/sales_item_cancel_return.json")
    orderData = sqlContext.read.json("src/test/resources/campaign/sales_order_cancel_return.json")
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }

  "empty order data " should "return empty data from execute function" in {
    val skuData = cancelRetarget.skuFilter(null)
    assert(skuData == null)
  }

  "Last day order items data " should "return empty data from execute function" in {
    val skuData = cancelRetarget.skuFilter(orderItemDataFrame)
    assert(skuData.count() == 2)
  }

  "Last day order items data for campaigns " should "return reference skus" in {
    val returnCancel = new ReturnCancel()

    val customerSelectedData = returnCancel.customerSelection(orderData, orderItemDataFrame)
    val skuData = cancelRetarget.skuFilter(customerSelectedData)
    assert(skuData.count() == 1)
  }

}
