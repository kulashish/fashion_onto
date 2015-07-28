package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 24/7/15.
 */
class YesterdaySessionDistinctTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerSurfData: DataFrame = _

  var yesterdaySessionDistinct: YesterdaySessionDistinct = _

  override def beforeAll() {

    super.beforeAll()
    yesterdaySessionDistinct = new YesterdaySessionDistinct()
    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_surf_data")
    dfCustomerSurfData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.CUSTOMER_SELECTION, DataSets.CUSTOMER_PAGE_VISIT, Schema.customerPageVisitSkuListLevel)

  }

  "YesterdaySessionDistinct: Data Frame yesterdaySessionDistinct" should "null" in {

    val result = yesterdaySessionDistinct.customerSelection(null)

    assert(result == null)

  }

  "YesterdaySessionDistinct: count " should "79" in {

    val result = yesterdaySessionDistinct.customerSelection(dfCustomerSurfData)

    assert(result.count() == 79)
  }

}
