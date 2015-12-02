package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ TestSchema, SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by raghu on 9/9/15.
 */
class NewArrivalsBrandCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var last30DaySalesOrderData: DataFrame = _
  @transient var last30DaySalesOrderItemData: DataFrame = _
  @transient var salesCart30Days: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    salesCart30Days = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "sales_cart", Schema.salesCart)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "itr", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/new_arrivals_brand", "brand_mvp_recommendations")
  }

  feature("Generate New Arrivals Brand"){
    scenario("Yesterday New Arrivals Brand"){
      Given("salesCart30Days, recommendationsData, yesterdayItrData")
      val newArrivalsBrandCampaign = new NewArrivalsBrandCampaign()
      val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
      newArrivalsBrandCampaign.runCampaign(last30DaySalesOrderData, last30DaySalesOrderItemData, salesCart30Days, recommendationsData, yesterdayItrData, yestDate)

      val NewArrivalsBrandCampaignOut = CampaignOutput.testData.head
      //      assert(NewArrivalsBrandCampaignOut._1.count() == 1)
      assert(NewArrivalsBrandCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && NewArrivalsBrandCampaignOut._2 == CampaignCommon.NEW_ARRIVALS_BRAND)

    }
  }
}
