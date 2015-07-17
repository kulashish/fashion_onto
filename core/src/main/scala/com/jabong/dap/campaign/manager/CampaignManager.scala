package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.campaignlist.LiveRetargetCampaign
import com.jabong.dap.common.Spark
import org.apache.spark.SparkConf

/**
 *  Campaign Manager will run multiple campaign based On Priority
 *  TODO: this class will need to be refactored to create a proper data flow of campaigns
 *
 */
object CampaignManager {

  //  def main(args: Array[String]) {
  //    val liveRetargetCampaign = new LiveRetargetCampaign()
  //    val conf = new SparkConf().setAppName("CampaignTest").set("spark.driver.allowMultipleContexts", "true")
  //
  //    Spark.init(conf)
  //    val hiveContext = Spark.getHiveContext()
  //    val orderData = hiveContext.read.parquet(args(0))
  //    val orderItemData = hiveContext.read.parquet(args(1))
  //    liveRetargetCampaign.runCampaign(orderData, orderItemData)
  //  }
  //
  //  def execute() = {
  //
  //    val liveRetargetCampaign = new LiveRetargetCampaign()
  //    liveRetargetCampaign.runCampaign(null)
  //
  //  }

}
