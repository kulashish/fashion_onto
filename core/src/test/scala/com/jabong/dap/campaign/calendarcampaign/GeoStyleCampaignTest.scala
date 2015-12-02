package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by samathashetty on 17/11/15.
 */
class GeoStyleCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var salesOrderItemData: DataFrame = _
  @transient var salesOrderData: DataFrame = _
  @transient var recommendationsData: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _
  @transient var salesAddressData: DataFrame = _
  @transient var cityWiseData: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()
    salesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "sales_order_item", Schema.salesOrderItem)
    salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "sales_order", Schema.salesOrder)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "itr", TestSchema.basicSimpleItr)
    recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "recommendation")
    salesAddressData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "sales_order_address")

    cityWiseData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/geo_campaign", "city_data", TestSchema.cityMapSchema)
  }

  feature("Run geo style campaign") {
    scenario("generate recommendations with city wise bricks") {
      Given("cityWiseData,salesOrder, salesOrderItemData, salesAddressData, yesterdayItrData, brickMvpRecommendation")
      val geoStyleCampaign = new GeoStyleCampaign()

      geoStyleCampaign.runCampaign(salesOrderData, salesOrderItemData, salesAddressData, yesterdayItrData, cityWiseData, recommendationsData, TimeUtils.YESTERDAY_FOLDER)

    }
  }

}
