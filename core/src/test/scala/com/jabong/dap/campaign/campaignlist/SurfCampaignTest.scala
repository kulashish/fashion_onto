package com.jabong.dap.campaign.campaignlist

import java.io.File

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CampaignCommon }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.{ TestConstants, TestSchema, Spark, SharedSparkContext }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.{ GivenWhenThen, FeatureSpec }

/**
 * Created by raghu on 23/9/15.
 */

class SurfCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  @transient var sqlContext: SQLContext = _

  @transient var yesterdaySalesOrderItemData: DataFrame = _
  @transient var last30DaySalesOrderItemData: DataFrame = _
  @transient var yesterdaySalesOrderData: DataFrame = _
  @transient var last30DaySalesOrderData: DataFrame = _
  @transient var brickMvpRecommendations: DataFrame = _
  @transient var yestSurfSessionData: DataFrame = _
  @transient var yestItrSkuData: DataFrame = _
  @transient var customerMasterData: DataFrame = _
  @transient var lastDaySurf3Data: DataFrame = _

  override def beforeAll() {

    super.beforeAll()
    CampaignOutput.setTestMode(true)
    sqlContext = Spark.getSqlContext()

    yestSurfSessionData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", TestConstants.CUSTOMER_PAGE_VISIT, TestSchema.customerPageVisitSkuListLevel)
    yestItrSkuData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", TestConstants.ITR_30_DAY_DATA, Schema.itr)
    customerMasterData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.SKU_SELECTION + File.separator + SkuSelection.SURF, DataSets.CUSTOMER, Schema.customer)
    yesterdaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", "sales_order", Schema.salesOrder)
    yesterdaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", "sales_order_item", Schema.salesOrderItem)
    lastDaySurf3Data = JsonUtils.readFromJson(DataSets.CAMPAIGNS + File.separator + TestConstants.CUSTOMER_SELECTION, TestConstants.CUSTOMER_PAGE_VISIT, TestSchema.customerPageVisitSkuListLevel)
    last30DaySalesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", "sales_order", Schema.salesOrder)
    last30DaySalesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", "sales_order_item", Schema.salesOrderItem)
    brickMvpRecommendations = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/surf", "brick_mvp_recommendations")

  }

  feature("Run surf campaign") {
    scenario("run all surf campaigns") {
      Given(" past30DayCampaignMergedData, yestSurfSessionData, yestItrSkuData, customerMasterData, yesterdaySalesOrderData, yesterdaySalesOrderItemData, lastDaySurf3Data, last30DaySalesOrderData, last30DaySalesOrderItemData, brickMvpRecommendations")

      val surfCampaign = new SurfCampaign()
      val incrDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)



      surfCampaign.runCampaign(
        yestSurfSessionData,
        yestItrSkuData,
        customerMasterData,
        yesterdaySalesOrderData,
        yesterdaySalesOrderItemData,
        lastDaySurf3Data,
        last30DaySalesOrderData,
        last30DaySalesOrderItemData,
        brickMvpRecommendations,
        incrDate
      )

      val surf1PushCampaignOut = CampaignOutput.testData(0)
      val surf1EmailCampaignOut = CampaignOutput.testData(1)
      assert(surf1PushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && surf1PushCampaignOut._2 == CampaignCommon.SURF1_CAMPAIGN)
      assert(surf1PushCampaignOut._1.count() == 2)
      assert(surf1EmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && surf1EmailCampaignOut._2 == CampaignCommon.SURF1_CAMPAIGN)
      //      assert(surf1EmailCampaignOut._1.count() == 0)

      val surf2PushCampaignOut = CampaignOutput.testData(2)
      val surf2EmailCampaignOut = CampaignOutput.testData(3)
      assert(surf2PushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && surf2PushCampaignOut._2 == CampaignCommon.SURF2_CAMPAIGN)
      assert(surf2PushCampaignOut._1.count() == 2)
      assert(surf2EmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && surf2EmailCampaignOut._2 == CampaignCommon.SURF2_CAMPAIGN)
      //      assert(surf2EmailCampaignOut._1.count() == 1)

      val surf6PushCampaignOut = CampaignOutput.testData(4)
      val surf6EmailCampaignOut = CampaignOutput.testData(5)
      assert(surf6PushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && surf6PushCampaignOut._2 == CampaignCommon.SURF6_CAMPAIGN)
      assert(surf6PushCampaignOut._1.count() == 1)
      assert(surf6EmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && surf6EmailCampaignOut._2 == CampaignCommon.SURF6_CAMPAIGN)
      //      assert(surf6EmailCampaignOut._1.count() == 1)

      val surf3PushCampaignOut = CampaignOutput.testData(6)
      val surf3EmailCampaignOut = CampaignOutput.testData(7)
      assert(surf3PushCampaignOut._3 == DataSets.PUSH_CAMPAIGNS && surf3PushCampaignOut._2 == CampaignCommon.SURF3_CAMPAIGN)
      assert(surf3PushCampaignOut._1.count() == 1)
      assert(surf3EmailCampaignOut._3 == DataSets.EMAIL_CAMPAIGNS && surf3EmailCampaignOut._2 == CampaignCommon.SURF3_CAMPAIGN)
      assert(surf3EmailCampaignOut._1.count() == 1)
    }
  }

}
