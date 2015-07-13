package com.jabong.dap.campaign.data

import com.jabong.dap.common.Spark
import org.apache.spark.sql.DataFrame

object CampaignOutput {

  /**
   * List of (customerID, ref skus, recommendations)
   * @param campaignOutput
   */
  def saveCampaignData(campaignOutput: DataFrame, outPath: String) = {
    campaignOutput.write.parquet(outPath)
  }
}
