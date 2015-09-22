package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
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
    shortlist3rdDayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/customer_product_shortlist", "customer_product_shortlist")
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "itr")
  }

  feature("Generate Shortlist Reminder Data"){
    scenario("3rd day Shortlist Reminder Data"){
      Given("shortlist3rdDayData, recommendationsData, yesterdayItrData")
      val shortlistReminderCampaign = new ShortlistReminderCampaign()
      shortlistReminderCampaign.runCampaign(shortlist3rdDayData, recommendationsData, yesterdayItrData)
    }
  }
}