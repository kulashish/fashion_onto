package com.jabong.dap.campaign.data

import org.apache.spark.sql.DataFrame


object CampaignOutput {

  /**
   * List of (customerID, ref skus, recommendations)
   * @param campaignOutput
   */
  def saveCampaignData(campaignOutput: DataFrame) = {
  }
}
