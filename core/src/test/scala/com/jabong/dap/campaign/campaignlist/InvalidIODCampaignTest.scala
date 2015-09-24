package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestSchema, Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ GivenWhenThen, FeatureSpec }
import com.jabong.dap.data.storage.schema.Schema

/**
 * Created by kapil on 21/9/15.
 */
class InvalidIODCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {
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

    salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "sales_order_invalid", Schema.salesOrder)

    //    salesCartData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/acart_campaigns", "sales_cart", Schema.salesCart)
    salesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "sales_item_invalid", Schema.salesOrderItem)
    //salesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS +"/acart_campaigns", "sales_order_item", Schema.salesOrderItem)
    // salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/acart_campaigns", "sales_order", Schema.salesOrder)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/invalid_campaigns", "itr_followup", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "brick_mvp_recommendations")
  }

  feature("Run invalid IOD campaigns") {
    scenario("Customer has invalid IOD the cart yesterday") {
      Given("salesOrderData,salesOrderItemData, yesterdayItrData, brickMvpRecommendation")
      val invalidIODCampaign = new InvalidIODCampaign()
      invalidIODCampaign.runCampaign(salesOrderData, salesOrderItemData, yesterdayItrData, recommendationsData)

      val invalidIODCampaignOut = CampaignOutput.testData.head
      val resultDF = invalidIODCampaignOut._1
      assert(invalidIODCampaignOut._3 == "email_campaigns" && invalidIODCampaignOut._2 == "invalid_iod")
    }
  }
}
