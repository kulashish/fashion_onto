package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.SharedSparkContext
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

  var itemOnDiscount: ItemOnDiscount = _

  override def beforeAll() {

    super.beforeAll()
    itemOnDiscount = new ItemOnDiscount()
    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_product_shortlist")
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.YESTERDAY_ITR_DATA, Schema.itr)
  }

  //==================================getJoinDF()=======================================================================
  "getJoinDF: Data Frame dfCustomerProductShortlist and dfItr30DayData" should "not null" in {

    var itr = dfItr30DayData

    itr = itr.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT,
      col(ItrVariables.SPECIAL_PRICE))

    val result = itemOnDiscount.getJoinDF(dfCustomerProductShortlist, itr)
      .limit(30).collect().toSet

    //                           result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_get_join_df" + ".json")

    val dfJoin = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.ITEM_ON_DISCOUNT, "result_get_join_df", Schema.resultGetJoin)
      .collect().toSet

    assert(result.equals(dfJoin) == true)

  }

  //=====================================shortListSkuSimpleFilter()=====================================================

  "shortListSkuSimpleFilter: Data Frame" should "match to resultant Data Frame" in {

    var yesterdayItrData = dfYesterdayItrData

    yesterdayItrData = yesterdayItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val result = itemOnDiscount.shortListSkuSimpleFilter(dfCustomerProductShortlist, yesterdayItrData)
      .limit(30).collect().toSet

    //                   result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_simple_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.ITEM_ON_DISCOUNT, "result_shortlist_sku_simple_filter", Schema.resultSkuSimpleFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

  //==========================================shortListSkuFilter()======================================================

  "shortListSkuFilter: Data Frame" should "match to resultant Data Frame" in {

    var itr30Day = dfItr30DayData

    itr30Day = dfItr30DayData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    var yesterdayItrData = dfYesterdayItrData

    yesterdayItrData = yesterdayItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    val result = itemOnDiscount.shortListSkuFilter(dfCustomerProductShortlist, yesterdayItrData, itr30Day)
      .limit(30).collect().toSet

    //                           result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_filter" + ".json")

    val dfShortListSkuFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.ITEM_ON_DISCOUNT, "result_shortlist_sku_filter", Schema.resultSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuFilter) == true)

  }

  //=====================================shortListFullSkuFilter()=====================================================

  "shortListFullSkuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = itemOnDiscount.shortListFullSkuFilter(dfCustomerProductShortlist, dfItr30DayData, dfYesterdayItrData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_full_sku_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.ITEM_ON_DISCOUNT, "result_shortlist_full_sku_filter", Schema.resultFullSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

  //=====================================skuFilter()=====================================================
  "skuFilter: Data Frame dfCustomerProductShortlist and dfItr30DayData" should "null" in {

    val result = itemOnDiscount.skuFilter(null, null)

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

    val result = itemOnDiscount.skuFilter(dfCustomerProductShortlist, dfItr30DayData)
    //        .limit(30).colle//ct().toSet

    //    result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_sku_filter" + ".json")

    //    val dfSkuFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" +  DataSets.ITEM_ON_DISCOUNT, "result_sku_filter", Schema.resultSkuSimpleFilter)
    //      .collect().toSet

    assert(result != null)

  }

}