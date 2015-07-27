package com.jabong.dap.campaign.utils

import com.jabong.dap.campaign.manager.CampaignManager
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
 * Created by rahul for com.jabong.dap.campaign.utils on 24/7/15.
 */
object CampaignUdfs {

  // udf to return the priority of campaign given campaign mail type
  //val campaignPriority = udf((mailType: Int) => CampaignUtils.getCampaignPriority(mailType: Int, CampaignManager.mailTypePriorityMap: mutable.HashMap[Int, Int]))

}
