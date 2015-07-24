package com.jabong.dap.campaign.utils

import org.apache.spark.sql.functions._

/**
 * Created by rahul for com.jabong.dap.campaign.utils on 24/7/15.
 */
object CampaignUdfs {

  val campaignPriority = udf((mailType: Int) => CampaignUtils.getCampaignPriority(mailType: Int))

}
