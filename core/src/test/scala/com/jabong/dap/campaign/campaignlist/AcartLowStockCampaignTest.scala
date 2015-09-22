package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{SharedSparkContext, Spark, TestSchema}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
  * Created by rahul on 22/9/15.
  */
class AcartLowStockCampaignTest extends FeatureSpec with GivenWhenThen with SharedSparkContext {

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
     salesOrderItemData = JsonUtils.readFromJson(DataSets.CAMPAIGNS +"/acart_campaigns", "sales_order_item", Schema.salesOrderItem)
     salesOrderData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/acart_campaigns", "sales_order", Schema.salesOrder)
     yesterdayItrData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "sku_simple_common_itr", TestSchema.basicSimpleItr)
     recommendationsData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/email_campaigns", "brick_mvp_recommendations")
   }

   feature("Run acart low stock campaigns") {
     scenario("Customer has abondoned the cart in the last 30 days and the ref sku has low stock now") {
       Given("salescartData,salesOrder, salesOrderItemData, yesterdayItrData, brickMvpRecommendation")
       val acartLowStockCampaign = new AcartLowStockCampaign()
       acartLowStockCampaign.runCampaign(salesCartData, salesOrderData, salesOrderItemData, yesterdayItrData, recommendationsData)
       val acartPushCampaignOut = CampaignOutput.testData.head
       val acartEmailCamapignOut = CampaignOutput.testData(1)
       assert(acartPushCampaignOut._3 == "push_campaigns" && acartPushCampaignOut._2 == "acart_lowstock")
       assert(acartPushCampaignOut._1.count() == 0)
       assert(acartEmailCamapignOut._3 == "email_campaigns" && acartEmailCamapignOut._2 == "acart_lowstock")
     }
   }

 }
