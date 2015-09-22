package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.{TestSchema, SharedSparkContext, Spark}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{GivenWhenThen, FeatureSpec}

/**
 * Created by rahul on 18/9/15.
 */
class LiveRetargetCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var salesOrderItemData: DataFrame = _
  @transient var salesOrderData: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()
    salesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "sales_item", Schema.salesOrderItem)
    salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "sales_order", Schema.salesOrder)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "sku_simple_common_itr", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "brick_mvp_recommendations")
  }

  feature("Run live cancel and return retarget campaigns") {
    scenario("Order which has been cancelled ") {
      Given("salesOrder, salesOrderItemData, yesterdayItrData, brickMvpRecommendation")
      val liveRetargetCampaign = new LiveRetargetCampaign()
      liveRetargetCampaign.runCampaign(salesOrderData, salesOrderItemData, yesterdayItrData, recommendationsData)
      val cancelRetargetOutput = CampaignOutput.testData.head
      val returnRetargetOutput = CampaignOutput.testData(2)
      assert(cancelRetargetOutput._3 == "push_campaigns" && cancelRetargetOutput._2 == "cancel_retarget")
      assert(cancelRetargetOutput._1.count() == 2)
      assert(returnRetargetOutput._3 == "push_campaigns" && returnRetargetOutput._2 == "return_retarget")
      assert(returnRetargetOutput._1.count() == 0)
    }
  }
}
