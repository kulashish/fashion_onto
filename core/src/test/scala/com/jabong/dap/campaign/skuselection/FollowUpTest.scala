package com.jabong.dap.campaign.skuselection

import java.io.File

import com.jabong.dap.common.constants.campaign.{ SkuSelection, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestSchema, TestConstants, SharedSparkContext, Spark }
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

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    customerSelected = JsonUtils.readFromJson(DataSets.CAMPAIGNS, "invalid_campaigns/invalid_followup_customer_select")
    itrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS, "invalid_campaigns/itr_followup")

    dfCustomerProductShortlist = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.RESULT_CUSTOMER_PRODUCT_SHORTLIST, TestSchema.resultCustomerProductShortlist)
    dfItr30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.ITR_30_DAY_DATA, Schema.itr)
    dfYesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION, TestConstants.YESTERDAY_ITR_DATA, Schema.itr)

  }

  "empty customer selected data " should "return empty ref skus" in {
    val skuData = FollowUp.skuFilter(null, itrData)
    assert(skuData == null)
  }

  "empty itrData selected data " should "return empty ref skus" in {
    val skuData = FollowUp.skuFilter(customerSelected, null)
    assert(skuData == null)
  }
  //FIXME: change the test cases to pass
  "Invalid customer selected data and last days itr " should "ref skus of fk_customer" in {
    val refSkus = FollowUp.skuFilter(customerSelected, itrData)
    val refSkuList = refSkus.filter(CustomerVariables.FK_CUSTOMER + " = " + 8552648)
      .select(ProductVariables.SKU_SIMPLE).collect().toString
    val expectedData = "ES418WA79UAUINDFAS"
    assert(refSkus.count() == 1)
    //  assert(refSkuList.head === expectedData)

  }

  //=====================================skuFilter()=====================================================
  /* FIXME
  "skuFilter: Data Frame" should "match to resultant Data Frame" in {

    val result = FollowUp.skuFilter(dfCustomerProductShortlist, dfItr30DayData, dfYesterdayItrData)
      .limit(30).collect().toSet

    //                       result.limit(30).write.json(DataSets.TEST_RESOURCES + "result_shortlist_full_sku_filter" + ".json")

    val dfShortListSkuSimpleFilter = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.ITEM_ON_DISCOUNT, "result_shortlist_full_sku_filter", TestSchema.resultFullSkuFilter)
      .collect().toSet

    assert(result.equals(dfShortListSkuSimpleFilter) == true)

  }*/

}
