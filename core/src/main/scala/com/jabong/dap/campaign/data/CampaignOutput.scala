package com.jabong.dap.campaign.data

import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import org.apache.spark.sql.DataFrame

object CampaignOutput {

  /**
   * List of (customerID, ref skus, recommendations)
   * @param campaignOutput
   */
  def saveCampaignData(campaignOutput: DataFrame, outPath: String) = {
    campaignOutput.write.parquet(outPath)
  }

  def saveCampaignDataForYesterday(campaignOutput: DataFrame, campaignName: String) = {

    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")

    val path = PathBuilder.buildPath(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, campaignName, DataSets.DAILY_MODE, dateYesterday)

    val itrExits = DataVerifier.dataExists(path)

    if (!itrExits) {
      campaignOutput.write.parquet(path)
    }

  }
}

