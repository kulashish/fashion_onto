package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ FeatureSpec, GivenWhenThen }

/**
 * Created by raghu on 22/9/15.
 */

class WishListCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var yesterdaySalesOrderItemData: DataFrame = _
  @transient var last30DaySalesOrderItemData: DataFrame = _
  @transient var yesterdaySalesOrderData: DataFrame = _
  @transient var last30DaySalesOrderData: DataFrame = _
  @transient var shortlistYesterdayData: DataFrame = _
  @transient var shortlistLast30DayData: DataFrame = _
  @transient var itrSkuSimpleYesterdayData: DataFrame = _
  @transient var itrSkuYesterdayData: DataFrame = _
  @transient var itrSku30DayData: DataFrame = _
  @transient var brickMvpRecommendations: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()
    shortlistYesterdayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "customer_product_shortlist", Schema.customerProductShortlist)
    shortlistLast30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "customer_product_shortlist", Schema.customerProductShortlist)
    yesterdaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "sales_order_item", Schema.salesOrderItem)
    last30DaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "sales_order_item", Schema.salesOrderItem)
    yesterdaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "sales_order", Schema.salesOrder)
    last30DaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "sales_order", Schema.salesOrder)
    itrSkuYesterdayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "basic_sku_itr", TestSchema.basicItr)
    itrSku30DayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "basic_sku_itr", TestSchema.basicItr)
    itrSkuSimpleYesterdayData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/wishlist", "itr_sku_simple", TestSchema.basicSimpleItr)
    brickMvpRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/mipr", "brick_mvp_recommendations")
  }

  feature("Run wishlist campaign") {
    scenario("run all wishlist campaigns") {
      Given("shortlistYesterdayData, shortlistLast30DayData, itrSkuYesterdayData, itrSkuSimpleYesterdayData,  yesterdaySalesOrderData, yesterdaySalesOrderItemData,  last30DaySalesOrderData, last30DaySalesOrderItemData, itrSku30DayData")

      val wishListCampaign = new WishListCampaign()

      wishListCampaign.runCampaign(
        shortlistYesterdayData,
        shortlistLast30DayData,
        itrSkuYesterdayData,
        itrSkuSimpleYesterdayData,
        yesterdaySalesOrderData,
        yesterdaySalesOrderItemData,
        last30DaySalesOrderData,
        last30DaySalesOrderItemData,
        itrSku30DayData,
        brickMvpRecommendations)

      val wishlistFollowupPushCampaignOut = CampaignOutput.testData(0)
      val wishlistFollowupEmailCampaignOut = CampaignOutput.testData(1)
      assert(wishlistFollowupPushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && wishlistFollowupPushCampaignOut._2 == CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN)
      assert(wishlistFollowupPushCampaignOut._1.count() == 2)
      assert(wishlistFollowupEmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && wishlistFollowupEmailCampaignOut._2 == CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN)
//      assert(wishlistFollowupEmailCampaignOut._1.count() == 0)

      val wishlistLowstockPushCampaignOut = CampaignOutput.testData(2)
      val wishlistLowstockEmailCampaignOut = CampaignOutput.testData(3)
      assert(wishlistLowstockPushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && wishlistLowstockPushCampaignOut._2 == CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN)
      assert(wishlistLowstockPushCampaignOut._1.count() == 2)
      assert(wishlistLowstockEmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && wishlistLowstockEmailCampaignOut._2 == CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN)
//      assert(wishlistLowstockEmailCampaignOut._1.count() == 1)

      val wishlistIODPushCampaignOut = CampaignOutput.testData(4)
      val wishlistIODEmailCampaignOut = CampaignOutput.testData(5)
      assert(wishlistIODPushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && wishlistIODPushCampaignOut._2 == CampaignCommon.WISHLIST_IOD_CAMPAIGN)
      assert(wishlistIODPushCampaignOut._1.count() == 1)
      assert(wishlistIODEmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && wishlistIODEmailCampaignOut._2 == CampaignCommon.WISHLIST_IOD_CAMPAIGN)
//      assert(wishlistIODEmailCampaignOut._1.count() == 1)

    }
  }

}
