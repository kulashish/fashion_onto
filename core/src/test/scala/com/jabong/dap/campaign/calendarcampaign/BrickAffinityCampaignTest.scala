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
 * Created by raghu on 17/11/15.
 */
class BrickAffinityCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var customerSurfAffinity: DataFrame = _
  @transient var last7thDaySalesOrderData: DataFrame = _
  @transient var last7thDaySalesOrderItemData: DataFrame = _
  @transient var brickMvpRecommendations: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    customerSurfAffinity = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brick_affinity", "customer_surf_affinity", Schema.surfAffinitySchema)
    last7thDaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brick_affinity", "sales_order", Schema.salesOrder)
    last7thDaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brick_affinity", "sales_order_item", Schema.salesOrderItem)
    brickMvpRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brick_affinity", "brick_mvp_recommendations")
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brick_affinity", "itr", TestSchema.basicSimpleItr)

  }

  feature("Generate brick Affinity Campaign"){
    scenario("select brick Affinity Campaign"){
      Given("customerSurfAffinity, last7thDaySalesOrderData, last7thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData")
      val brickAffinityCampaign = new BrickAffinityCampaign()
      brickAffinityCampaign.runCampaign(customerSurfAffinity, last7thDaySalesOrderData, last7thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData)

      val brickAffinityCampaignOut = CampaignOutput.testData.head
      //      assert(pricepointCampaignOut._1.count() == 1)
      assert(brickAffinityCampaignOut._3 == DataSets.CALENDAR_CAMPAIGNS && brickAffinityCampaignOut._2 == CampaignCommon.BRICK_AFFINITY_CAMPAIGN)

    }
  }
}
