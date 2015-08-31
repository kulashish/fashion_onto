package com.jabong.dap.campaign.skuselection

/**
 * Created by raghu on 18/7/15.
 */

import java.io.File

import com.jabong.dap.common.{ TestSchema, TestConstants, SharedSparkContext }
import com.jabong.dap.common.constants.campaign.SkuSelection
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 17/7/15.
 */
class SkuLowStockTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  var skuLowStock: SkuLowStock = _

  override def beforeAll() {

    super.beforeAll()
    skuLowStock = new SkuLowStock()
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, TestSchema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.YESTERDAY_ITR_DATA, Schema.itr)
  }

  //=====================================shortListSkuFilter()=====================================================
  "shortListSkuFilter: Data Frame dfCustomerProductShortlist and dfItr30DayData" should "null" in {

    val result = skuLowStock.shortListSkuFilter(null, null)

    assert(result == null)

  }

  "shortListSkuFilter: schema attributes and data type" should
    "match into dfCustomerProductShortlist and dfItr30DayData" in {

      val result = skuLowStock.shortListSkuFilter(dfCustomerProductShortlist, dfItr30DayData)
      assert(result != null)

    }

  "shortListSkuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = skuLowStock.shortListSkuFilter(dfCustomerProductShortlist, dfItr30DayData)
      .limit(30).collect().toSet

    //                           result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_filter" + ".json")

    val dfSkuFilter = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.LOW_STOCK, "result_shortlist_sku_filter", TestSchema.resultSkuFilter)
      .collect().toSet

    assert(result != null)

  }

  //=====================================shortListSkuSimpleFilter()=====================================================

  "shortListSkuSimpleFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = skuLowStock.shortListSkuSimpleFilter(dfCustomerProductShortlist, dfItr30DayData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_simple_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.LOW_STOCK, "result_shortlist_sku_simple_filter", TestSchema.resultSkuSimpleFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

  //=====================================skuFilter()=====================================================

  "skuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = skuLowStock.skuFilter(dfCustomerProductShortlist, dfItr30DayData, dfYesterdayItrData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_full_sku_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.LOW_STOCK, "result_shortlist_full_sku_filter", TestSchema.resultFullSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

}