package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
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
    last30DaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "sales_order")
    yesterdaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "sales_order_item")
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "itr")
  }

  feature("Generate All closed item"){
    scenario("In order: All item status closed"){
      Given("last30DaySalesOrderData, yesterdaySalesOrderItemData, recommendationsData, yesterdayItrData")
      val miprCampaign = new MIPRCampaign()
      miprCampaign.runCampaign(last30DaySalesOrderData, yesterdaySalesOrderItemData, recommendationsData, yesterdayItrData)
    }
  }
}
