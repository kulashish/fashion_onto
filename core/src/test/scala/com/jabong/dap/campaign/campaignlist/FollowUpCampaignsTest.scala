package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by rahul on 13/10/15.
 */
class FollowUpCampaignsTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var emailCampaignMergedData: DataFrame = _
  @transient var salesOrderData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()
    salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/follow_up_campaigns", "sales_order_data", Schema.salesOrder)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/follow_up_campaigns", "basic_sku_itr", TestSchema.basicItr)
    emailCampaignMergedData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/follow_up_campaigns", "email_campaign_merged")
  }

  feature("Run follow up campaigns") {
    scenario("We have run follow up campaigns for surf, cancel and return  campaigns ") {
      Given("emailCampaignMergedData,salesOrder, yesterdayItrData")
      val followUpCampaigns = new FollowUpCampaigns()
      followUpCampaigns.runCampaign(emailCampaignMergedData, salesOrderData, yesterdayItrData, TimeUtils.YESTERDAY_FOLDER)
      val followUpCampaignsOut = CampaignOutput.testData.head
      assert(followUpCampaignsOut._3 == "email_campaigns" && followUpCampaignsOut._2 == "follow_up_campaigns")
    }
  }

}
