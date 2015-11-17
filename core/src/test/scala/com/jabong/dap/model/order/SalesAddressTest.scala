package com.jabong.dap.model.order

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.schema.OrderVarSchema
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

  //  override def afterAll() {
  //    super.afterAll()
  //  }
}