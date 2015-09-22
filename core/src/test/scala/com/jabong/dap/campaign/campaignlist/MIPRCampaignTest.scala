package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestSchema, Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ GivenWhenThen, FeatureSpec }

/**
 * Created by raghu on 14/9/15.
 */
class MIPRCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var last30DaySalesOrderData: DataFrame = _
  @transient var yesterdaySalesOrderItemData: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    last30DaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "sales_order", Schema.salesOrder)
    yesterdaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "sales_order_item", Schema.salesOrderItem)
    //    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "itr")
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "itr", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "brick_mvp_recommendations")
  }

  feature("Generate All closed item"){
    scenario("In order: All item status closed"){
      Given("last30DaySalesOrderData, yesterdaySalesOrderItemData, recommendationsData, yesterdayItrData")
      val miprCampaign = new MIPRCampaign()
      miprCampaign.runCampaign(last30DaySalesOrderData, yesterdaySalesOrderItemData, recommendationsData, yesterdayItrData)

      val MIPRCampaignOut = CampaignOutput.testData.head
      assert(MIPRCampaignOut._1.count() == 1)
      assert(MIPRCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && MIPRCampaignOut._2 == CampaignCommon.MIPR_CAMPAIGN)

    }
  }
}
