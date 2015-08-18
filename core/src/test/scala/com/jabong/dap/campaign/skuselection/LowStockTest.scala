package com.jabong.dap.campaign.skuselection

/**
 * Created by raghu on 18/7/15.
 */

import java.io.File

import com.jabong.dap.common.{TestConstants, SharedSparkContext}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 17/7/15.
 */
class LowStockTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  var lowStock: LowStock = _

  override def beforeAll() {

    super.beforeAll()
    lowStock = new LowStock()
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.YESTERDAY_ITR_DATA, Schema.itr)
  }

  //=====================================skuFilter()=====================================================
  "skuFilter: Data Frame dfCustomerProductShortlist and dfItr30DayData" should "null" in {

    val result = lowStock.skuFilter(null, null)

    assert(result == null)

  }

  //  "skuFilter: schema attributes and data type" should
  //    "match into dfCustomerProductShortlist and dfItr30DayData" in {
  //
  //      val result = lowStock.skuFilter(dfCustomerProductShortlist, dfItr30DayData)
  //      assert(result != null)
  //
  //    }

}