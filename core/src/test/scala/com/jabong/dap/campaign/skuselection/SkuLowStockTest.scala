package com.jabong.dap.campaign.skuselection

/**
 * Created by raghu on 18/7/15.
 */
import com.jabong.dap.common.SharedSparkContext
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
    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_product_shortlist")
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.YESTERDAY_ITR_DATA, Schema.itr)
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

    val dfSkuFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.LOW_STOCK, "result_shortlist_sku_filter", Schema.resultSkuFilter)
      .collect().toSet

    assert(result != null)

  }

  //=====================================shortListSkuSimpleFilter()=====================================================

  "shortListSkuSimpleFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = skuLowStock.shortListSkuSimpleFilter(dfCustomerProductShortlist, dfItr30DayData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_sku_simple_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.LOW_STOCK, "result_shortlist_sku_simple_filter", Schema.resultSkuSimpleFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

  //=====================================skuFilter()=====================================================

  "skuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = skuLowStock.skuFilter(dfCustomerProductShortlist, dfItr30DayData, dfYesterdayItrData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_full_sku_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.LOW_STOCK, "result_shortlist_full_sku_filter", Schema.resultFullSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

}