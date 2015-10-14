package com.jabong.dap.model.order

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import com.jabong.dap.model.order.schema.OrderVarSchema
import com.jabong.dap.model.order.variables.SalesOrder
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 29/6/15.
 */
class SalesOrderTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _
  @transient var dfSalesOrder: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order1", OrderVarSchema.salesOrder)
    df2 = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order2", OrderVarSchema.salesOrderCoupon)
    dfSalesOrder = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order", Schema.salesOrder)
  }

  "The result" should "have size 3" in {
    var ordersCount = SalesOrder.couponScore(df2)
    assert(ordersCount.collect.size == 3)
  }

  "getCPOT: Data Frame" should "match to resultant Data Frame" in {

    val result = SalesOrder.getCPOT(dfSalesOrder: DataFrame)
      .limit(30).collect().toSet

    //result.limit(30).write.json(DataSets.TEST_RESOURCES + "customers_preferred_order_timeslot" + ".json")

    val dfCustomersPreferredOrderTimeslot = JsonUtils.readFromJson(DataSets.CUSTOMER, "customers_preferred_order_timeslot",
      CustVarSchema.customersPreferredOrderTimeslotPart2)
      .collect().toSet

    //    result.collect().foreach(println)
    //    result.printSchema()
    //
    //    dfCustomersPreferredOrderTimeslot.collect().foreach(println)
    //    dfCustomersPreferredOrderTimeslot.printSchema()

    assert(result.equals(dfCustomersPreferredOrderTimeslot) == true)

  }

}
