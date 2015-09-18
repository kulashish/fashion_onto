package com.jabong.dap.campaign.data

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object CampaignOutput {

  var testMode: Boolean = false
  var testData: mutable.MutableList[(DataFrame, String, String)] = null

  def setTestMode(newMode: Boolean): Unit = {
    testMode = newMode
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

      val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

      val path = PathBuilder.buildPath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.DAILY_MODE, dateYesterday)

      val dataExits = DataVerifier.dataExists(path)

      if (!dataExits) {
        campaignOutput.write.parquet(path)
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

