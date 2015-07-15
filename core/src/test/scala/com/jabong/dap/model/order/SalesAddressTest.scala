package com.jabong.dap.model.order

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.schema.OrderVarSchema
import com.jabong.dap.model.order.variables.SalesOrderAddress
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 2/7/15.
 */
class SalesAddressTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson(DataSets.SALES_ORDER_ADDRESS, "sales_address", OrderVarSchema.salesOrderAddress)

    df1.collect.foreach(println)
  }

  "Testing max mobile No and city" should "have size 3" in {
    var maxCityPhon = SalesOrderAddress.getFav(df1)
    maxCityPhon.collect().foreach(println)
    assert(maxCityPhon.collect.size == 3)
  }

  //  override def afterAll() {
  //    super.afterAll()
  //  }
}