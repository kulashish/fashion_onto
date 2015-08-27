package com.jabong.dap.campaign.skuselection

import java.io.File

import com.jabong.dap.common.{TestSchema, TestConstants, SharedSparkContext}
import com.jabong.dap.common.constants.campaign.SkuSelection
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by raghu on 25/7/15.
 */
class SkuItemOnDiscountTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  var skuItemOnDiscount: SkuItemOnDiscount = _

  override def beforeAll() {

    super.beforeAll()

    skuItemOnDiscount = new SkuItemOnDiscount()
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.YESTERDAY_ITR_DATA, Schema.itr)
  }

  //=====================================skuFilter()=====================================================

  "skuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = skuItemOnDiscount.skuFilter(dfCustomerProductShortlist, dfItr30DayData, dfYesterdayItrData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_full_sku_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.ITEM_ON_DISCOUNT, "result_shortlist_full_sku_filter", TestSchema.resultFullSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

}
