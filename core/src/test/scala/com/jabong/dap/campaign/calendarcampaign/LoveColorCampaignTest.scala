package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestSchema, Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ GivenWhenThen, FeatureSpec }

/**
 * Created by rahul on 18/11/15.
 */
class LoveColorCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var last15thDaysSalesOrderData: DataFrame = _
  @transient var last15thDaysSalesOrderItemData: DataFrame = _
  @transient var mvpColorRecommendations: DataFrame = _
  @transient var customerTopData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    last15thDaysSalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/clearance", "sales_order", Schema.salesOrder)
    last15thDaysSalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/clearance", "sales_order_item", Schema.salesOrderItem)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/", "itr_sku_simple", TestSchema.basicSimpleItr)
    mvpColorRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign", "mvp_color_recommendation")
    customerTopData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/love_brand", "top_brand_color_sku", TestSchema.customerFavList)

  }

  feature("run love Color Campaign"){
    scenario("select sku from 20th day last order data"){
      Given("last15thDaysSalesOrderData, last15thDaysSalesOrderItemData, mvpColorRecommendations, yesterdayItrSkuSimpleData")
      val loveColorCampaign = new LoveColorCampaign()
      loveColorCampaign.runCampaign(customerTopData, last15thDaysSalesOrderData, last15thDaysSalesOrderItemData, mvpColorRecommendations, yesterdayItrData, "2015-11-13 23:43:43.0")

      val loveBrandCampaignOut = CampaignOutput.testData.head
      //      assert(pricepointCampaignOut._1.count() == 1)
      assert(loveBrandCampaignOut._3 == DataSets.CALENDAR_CAMPAIGNS && loveBrandCampaignOut._2 == CampaignCommon.LOVE_COLOR_CAMPAIGN)

    }
  }

}

