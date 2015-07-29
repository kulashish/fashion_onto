package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Created by rahul for com.jabong.dap.campaign.skuselection on 18/7/15.
 */
class FollowUpTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var customerSelected: DataFrame = _
  @transient var itrData: DataFrame = _

  @transient var dfCustomerProductShortlist: DataFrame = _
  @transient var dfItr30DayData: DataFrame = _
  @transient var dfYesterdayItrData: DataFrame = _

  var followUp: FollowUp = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    followUp = new FollowUp()
    customerSelected = sqlContext.read.json("src/test/resources/campaign/invalid_campaigns/invalid_followup_customer_select.json")
    itrData = sqlContext.read.json("src/test/resources/campaign/invalid_campaigns/itr_followup.json")

    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.RESULT_CUSTOMER_PRODUCT_SHORTLIST, Schema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION, DataSets.YESTERDAY_ITR_DATA, Schema.itr)

  }

  "empty customer selected data " should "return empty ref skus" in {
    val skuData = followUp.skuFilter(null, itrData)
    assert(skuData == null)
  }

  "empty itrData selected data " should "return empty ref skus" in {
    val skuData = followUp.skuFilter(customerSelected, null)
    assert(skuData == null)
  }
  //FIXME: change the test cases to pass
  "Invalid customer selected data and last days itr " should "ref skus of fk_customer" in {
    val refSkus = followUp.skuFilter(customerSelected, itrData)
    val refSkuList = refSkus.filter(CustomerVariables.FK_CUSTOMER + " = " + 8552648)
      .select(CampaignMergedFields.REF_SKU1).collect().toString
    val expectedData = "ES418WA79UAUINDFAS"
    assert(refSkus.count() == 1)
    //  assert(refSkuList.head === expectedData)

  }

  //=====================================skuFilter()=====================================================

  "skuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = followUp.skuFilter(dfCustomerProductShortlist, dfItr30DayData, dfYesterdayItrData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_full_sku_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGN + "/" + DataSets.SKU_SELECTION + "/" + DataSets.ITEM_ON_DISCOUNT, "result_shortlist_full_sku_filter", Schema.resultFullSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }

}
