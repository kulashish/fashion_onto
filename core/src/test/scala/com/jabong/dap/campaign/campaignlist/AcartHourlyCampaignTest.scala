package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.{ TestSchema, Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ GivenWhenThen, FeatureSpec }

/**
 * Created by rahul on 4/11/15.
 */
class AcartHourlyCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var salesOrderItemData: DataFrame = _
  @transient var salesOrderData: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var salesCartData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()
    salesCartData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/acart_campaigns", "sales_cart", Schema.salesCart)
    salesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/acart_campaigns", "sales_order_item", Schema.salesOrderItem)
    salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/acart_campaigns", "sales_order", Schema.salesOrder)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "sku_simple_common_itr", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "brick_mvp_recommendations")
  }

  feature("Run acart hourly campaign") {
    scenario("Customer has abondoned the cart one hour back") {
      Given("salescartData,salesOrder, salesOrderItemData, yesterdayItrData, brickMvpRecommendation")
      val acartDailyCampaign = new AcartDailyCampaign()
      acartDailyCampaign.runCampaign(salesCartData, salesOrderData, salesOrderItemData, yesterdayItrData, recommendationsData, TimeUtils.YESTERDAY_FOLDER)
      val acartEmailCamapignOut = CampaignOutput.testData.head
      assert(acartEmailCamapignOut._3 == "email_campaigns" && acartEmailCamapignOut._2 == "acart_hourly")
    }
  }

}
