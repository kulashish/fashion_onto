package com.jabong.dap.campaign.data

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
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

    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val path = PathBuilder.buildPath(ConfigConstants.OUTPUT_PATH, DataSets.CAMPAIGN, campaignName, DataSets.DAILY_MODE, dateYesterday)

    val dataExits = DataVerifier.dataExists(path)

    if (!dataExits) {
      campaignOutput.write.parquet(path)
    }

  }
}

