package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by raghu on 9/9/15.
 */
class NewArrivalsBrandCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var salesCart30Days: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    salesCart30Days = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "sales_cart")
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "itr")
  }

  feature("Generate New Arrivals Brand"){
    scenario("Yesterday New Arrivals Brand"){
      Given("salesCart30Days, recommendationsData, yesterdayItrData")
      val newArrivalsBrandCampaign = new NewArrivalsBrandCampaign()
      newArrivalsBrandCampaign.runCampaign(salesCart30Days, recommendationsData, yesterdayItrData)
    }
  }
}
