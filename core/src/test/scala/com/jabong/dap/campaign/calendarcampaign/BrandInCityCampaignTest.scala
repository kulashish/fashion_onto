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
class BrandInCityCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var last6thDaySalesOrderData: DataFrame = _
  @transient var last6thDaySalesOrderItemData: DataFrame = _
  @transient var brandMvpCityRecommendations: DataFrame = _
  @transient var yesterdayItrData: DataFrame = _
  @transient var fullCusTop5: DataFrame = _
  @transient var fullSalesOrderAddress: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    CampaignOutput.setTestMode(true)
    fullCusTop5 = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brand_in_city", "custTop5")
    fullSalesOrderAddress = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brand_in_city", "sales_order_address")
    last6thDaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brand_in_city", "sales_order", Schema.salesOrder)
    last6thDaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brand_in_city", "sales_order_item", Schema.salesOrderItem)
    yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brand_in_city", "itr", TestSchema.basicSimpleItr)
    brandMvpCityRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/calendarcampaign/brand_in_city", "brand_mvp_city")
  }

  feature("Generate customer favorite brand"){
    scenario("select customer favorite brand from last 6th day order history"){
      Given("last6thDaySalesOrderData, last6thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData")
      val brandInCityCampaign = new BrandInCityCampaign()

      brandInCityCampaign.runCampaign(fullSalesOrderAddress, last6thDaySalesOrderData, last6thDaySalesOrderItemData, brandMvpCityRecommendations, yesterdayItrData, "2015-11-13 23:43:43.0")

      val BrandInCityCampaignOut = CampaignOutput.testData.head
      //      assert(BrandInCityCampaignOut._1.count() == 1)
      assert(BrandInCityCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && BrandInCityCampaignOut._2 == CampaignCommon.BRAND_IN_CITY_CAMPAIGN)

    }
  }
}
