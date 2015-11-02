package com.jabong.dap.campaign.data

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object CampaignOutput {

  var testMode: Boolean = false
  var testData: mutable.MutableList[(DataFrame, String, String)] = null

  def setTestMode(newMode: Boolean): Unit = {
    testMode = newMode
    CampaignUtils.testMode = newMode
  }

  /**
   * List of (customerID, ref skus, recommendations)
   * @param campaignOutput
   */
  def saveCampaignData(campaignOutput: DataFrame, outPath: String) = {
    campaignOutput.write.parquet(outPath)
  }

  def saveCampaignDataForYesterday(campaignOutput: DataFrame, campaignName: String, campaignType: String = DataSets.PUSH_CAMPAIGNS) = {
    if (testMode) {
      saveTestData(campaignOutput, campaignName, campaignType)
    } else {
      if (campaignName.equals(CampaignCommon.ACART_HOURLY_CAMPAIGN)) {
        val dateYesterday = TimeUtils.getDateAfterHours(0, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)
        val path = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.HOURLY_MODE, dateYesterday)
        if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, path)) {
          DataWriter.writeParquet(campaignOutput, path, DataSets.IGNORE_SAVEMODE)
        }
        val acartHourlyFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_ACART_HOURLY"
        DataWriter.writeCsv(campaignOutput, campaignType, campaignName, DataSets.HOURLY_MODE, TimeUtils.LAST_HOUR_FOLDER, acartHourlyFileName, DataSets.IGNORE_SAVEMODE, "true", ";")
      } else {
        val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
        val path = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.DAILY_MODE, dateYesterday)

        if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, path)) {
          DataWriter.writeParquet(campaignOutput, path, DataSets.IGNORE_SAVEMODE)
        }
      }

    }
  }

  def saveTestData(campaignOutput: DataFrame, campaignName: String, campaignType: String): Unit = {
    if (null == testData) {
      testData = new mutable.MutableList[(DataFrame, String, String)]
    }
    testData += Tuple3(campaignOutput, campaignName, campaignType)
  }
}

