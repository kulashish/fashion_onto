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
class LowStockTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  var lowStock: LowStock = _

  override def beforeAll() {

    super.beforeAll()
    lowStock = new LowStock()
    //    JsonUtils.writeToJson("/home/raghu/bigData/parquetFiles/", "customer_product_shortlist")
    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.YESTERDAY_ITR_DATA, Schema.itr)
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