package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by raghu on 14/9/15.
 */
class ReplenishmentCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var fullSalesOrderData = _
  @transient var fullSalesOrderItemData = _
  @transient var brickMvpRecommendations = _
  @transient var yesterdayItrData = _
  @transient var yestCustomerData = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    yestCustomerData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/replenishment", "customer")
    fullSalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/replenishment", "sales_order", Schema.salesOrder)
    fullSalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/replenishment", "sales_order_item", Schema.salesOrderItem)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/replenishment", "itr", TestSchema.basicSimpleItr)
    brickMvpRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign", "brick_mvp_recommendations")
  }

  feature("Generate non_beauty_frag_campaign and beauty_campaign data "){
    scenario("generate Replenish data on the basis of product category "){
      Given("yestCustomerData, fullSalesOrderData, fullSalesOrderItemData, brickMvpRecommendations, yesterdayItrData")
      val replenishmentCampaign = new ReplenishmentCampaign()
      replenishmentCampaign.runCampaign(yestCustomerData, fullSalesOrderData, fullSalesOrderItemData, brickMvpRecommendations, yesterdayItrData)

      val nonBeautyReplenishmentCampaignOut = CampaignOutput.testData(0)
      //      assert(nonBeautyReplenishmentCampaignOut._1.count() == 1)
      assert(nonBeautyReplenishmentCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && nonBeautyReplenishmentCampaignOut._2 == CampaignCommon.NON_BEAUTY_FRAG_CAMPAIGN)

      val beautyReplenishmentCampaignOut = CampaignOutput.testData(1)
      //      assert(nonBeautyReplenishmentCampaignOut._1.count() == 1)
      assert(beautyReplenishmentCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && beautyReplenishmentCampaignOut._2 == CampaignCommon.BEAUTY_CAMPAIGN)

    }
  }
}

