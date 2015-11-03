package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 25/10/15.
 */
class CustTop5Test extends FlatSpec with SharedSparkContext {

  @transient var salesOrderItemJoined: DataFrame = _
  @transient var itr: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    salesOrderItemJoined = JsonUtils.readFromJson(DataSets.SALES_ORDER, "sales_order_item_joined")
    itr = JsonUtils.readFromJson(DataSets.SALES_ORDER, "Sales_itr")

  }

  "testing custTop5" should "match to resultant Data Frame" in {

    val df = CustTop5.getTop5(null, salesOrderItemJoined, itr)

    val res = CustTop5.calcTop5(df, null, "")

    res.collect().foreach(println)

    assert(res.collect().size > 2)

  }

}
