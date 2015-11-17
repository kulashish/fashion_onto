package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.{ TestSchema, Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ GivenWhenThen, FeatureSpec }

/**
 * Created by rahul on 10/11/15.
 */
class ClearanceCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var last30DaysSalesOrderData: DataFrame = _
  @transient var last30DaysSalesOrderItemData: DataFrame = _
  @transient var mvpDiscountRecommendations: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    last30DaysSalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/clearance", "sales_order", Schema.salesOrder)
    last30DaysSalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/clearance", "sales_order_item", Schema.salesOrderItem)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/", "itr_sku_simple", TestSchema.basicSimpleItr)
    mvpDiscountRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign", "mvp_discount_recommendation")
  }

  feature("run Clearance Campaign"){
    scenario("select sku from 20th day last order data"){
      Given("last30DaysSalesOrderData, last30DaysSalesOrderItemData, mvpDiscountRecommendations, yesterdayItrSkuSimpleData")
      val clearanceCampaign = new ClearanceCampaign()
      clearanceCampaign.runCampaign(last30DaysSalesOrderData, last30DaysSalesOrderItemData, mvpDiscountRecommendations, yesterdayItrData, "2015-11-13 23:43:43.0")

      val pricepointCampaignOut = CampaignOutput.testData.head
      //      assert(pricepointCampaignOut._1.count() == 1)
      assert(pricepointCampaignOut._3 == DataSets.CALENDAR_CAMPAIGNS && pricepointCampaignOut._2 == CampaignCommon.CLEARANCE_CAMPAIGN)

    }
  }
}