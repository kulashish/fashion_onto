package com.jabong.dap.campaign.data

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
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
    val path = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CAMPAIGNS, campaignName, DataSets.DAILY_MODE, dateYesterday)

    if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE,path)) {
      DataWriter.writeParquet(campaignOutput,path,DataSets.IGNORE_SAVEMODE)
    }

  }
}

