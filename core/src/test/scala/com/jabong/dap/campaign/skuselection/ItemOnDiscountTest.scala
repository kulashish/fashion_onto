package com.jabong.dap.campaign.skuselection

import java.io.File

import com.jabong.dap.common.{ TestSchema, TestConstants, SharedSparkContext }
import com.jabong.dap.common.constants.campaign.SkuSelection
import com.jabong.dap.common.constants.variables.ItrVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 17/7/15.
 */
class ItemOnDiscountTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  override def beforeAll() {

    super.beforeAll()
    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_product_shortlist")
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, TestSchema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.YESTERDAY_ITR_DATA, Schema.itr)
  }

  //==================================getJoinDF()=======================================================================
  "getJoinDF: Data Frame dfCustomerProductShortlist and dfItr30DayData" should "not null" in {

    var itr = dfItr30DayData

    itr = itr.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT,
      col(ItrVariables.SPECIAL_PRICE))

    val result = ItemOnDiscount.getJoinDF(dfCustomerProductShortlist, itr)
      .limit(30).collect().toSet

    //                           result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_get_join_df" + ".json")

    val dfJoin = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.ITEM_ON_DISCOUNT, "result_get_join_df", TestSchema.resultGetJoin)
      .collect().toSet

    assert(result.equals(dfJoin) == true)

  }

  //=====================================skuFilter()=====================================================
  "skuFilter: Data Frame dfCustomerProductShortlist and dfItr30DayData" should "null" in {

    val result = ItemOnDiscount.skuFilter(null, null)

    assert(result == null)

  }
  //
  //  "skuFilter: schema attributes and data type" should
  //    "match into dfCustomerProductShortlist and dfItr30DayData" in {
  //
  //      val result = itemOnDiscount.skuFilter(dfCustomerProductShortlist, dfItr30DayData, null)
  //      assert(result != null)
  //
  //    }

  "skuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = ItemOnDiscount.skuFilter(dfCustomerProductShortlist, dfItr30DayData)
    //        .limit(30).collect().toSet

    //    result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_sku_filter" + ".json")

    //    val dfSkuFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + File.separator + DataSets.SKU_SELECTION + File.separator +  DataSets.ITEM_ON_DISCOUNT, "result_sku_filter", Schema.resultSkuSimpleFilter)
    //      .collect().toSet

    assert(result != null)

  }

}