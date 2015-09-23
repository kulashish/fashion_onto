package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestSchema, SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by raghu on 14/9/15.
 */

class ShortlistReminderCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var shortlist3rdDayData: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    shortlist3rdDayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/shortlist_reminder", "customer_product_shortlist", Schema.customerProductShortlist)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/shortlist_reminder", "itr", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/shortlist_reminder", "brick_mvp_recommendations")
  }

  feature("Generate Shortlist Reminder Data"){
    scenario("3rd day Shortlist Reminder Data"){
      Given("shortlist3rdDayData, recommendationsData, yesterdayItrData")
      val shortlistReminderCampaign = new ShortlistReminderCampaign()
      shortlistReminderCampaign.runCampaign(shortlist3rdDayData, recommendationsData, yesterdayItrData)

      val ShortlistReminderCampaignOut = CampaignOutput.testData.head
      //      assert(ShortlistReminderCampaignOut._1.count() == 1)
      assert(ShortlistReminderCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && ShortlistReminderCampaignOut._2 == CampaignCommon.SHORTLIST_REMINDER)
    }
  }
}