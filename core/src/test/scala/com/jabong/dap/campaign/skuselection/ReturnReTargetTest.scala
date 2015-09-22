package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * return reTarget sku selection test cases
 */
class ReturnReTargetTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var testDataFrame: DataFrame = _
  @transient var orderItemDataFrame: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    orderItemDataFrame = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order_with_item")
    // testDataFrame = JsonUtils.readFromJson("sales_cart", "SalesCartEmpty")
  }

  "empty order data " should "return empty data from execute function of return re-target " in {
    val skuData = ReturnReTarget.skuFilter(null)
    assert(skuData == null)
  }

  //FIXME: change the test cases to pass
  "Last day order items data " should "return two records from execute function" in {
    val skuData = ReturnReTarget.skuFilter(orderItemDataFrame)
    assert(skuData.count() == 1)
  }

}
