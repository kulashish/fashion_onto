package com.jabong.dap.campaign.calendarcampaign

/**
 * Created by raghu on 29/9/15.
 */
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
class PricepointCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var last20thDaySalesOrderData: DataFrame = _
  @transient var last20thDaySalesOrderItemData: DataFrame = _
  @transient var brickMvpRecommendations: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    last20thDaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/pricepoint", "sales_order", Schema.salesOrder)
    last20thDaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/pricepoint", "sales_order_item", Schema.salesOrderItem)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/pricepoint", "itr", TestSchema.basicSimpleItr)
    brickMvpRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign", "brick_mvp_recommendations")
  }

  feature("Generate Price Point"){
    scenario("select sku from 20th day last order data"){
      Given("last20thDaySalesOrderData, last20thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData")
      val pricePointCampaign = new PricepointCampaign()
      pricePointCampaign.runCampaign(last20thDaySalesOrderData, last20thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData)

      val pricepointCampaignOut = CampaignOutput.testData.head
      //      assert(pricepointCampaignOut._1.count() == 1)
      assert(pricepointCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && pricepointCampaignOut._2 == CampaignCommon.PRICEPOINT_CAMPAIGN)

    }
  }
}
