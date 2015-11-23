package com.jabong.dap.campaign.data

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CampaignCommon }
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

  /**
   * save campaignsData
   * @param campaignOutput
   * @param campaignName
   * @param campaignType
   */
  def saveCampaignDataForYesterday(campaignOutput: DataFrame, campaignName: String, campaignType: String = DataSets.PUSH_CAMPAIGNS) = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    val path = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.DAILY_MODE, dateYesterday)

    if (testMode) {
      saveTestData(campaignOutput, campaignName, campaignType)
    } else {
      if (campaignName.equals(CampaignCommon.ACART_HOURLY_CAMPAIGN)) {
        val dateToday = TimeUtils.getDateAfterHours(0, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)
        val acartPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.HOURLY_MODE, dateToday)
        if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, acartPath)) {
          CampaignUtils.getAcartHourlyFields(campaignOutput)
          DataWriter.writeParquet(campaignOutput, acartPath, DataSets.IGNORE_SAVEMODE)
        }
        val campaignCsvOutput = campaignOutput
          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
          .drop(CampaignMergedFields.REC_SKUS)
          .drop(CampaignMergedFields.REF_SKUS)
          .drop(CampaignMergedFields.LIVE_MAIL_TYPE)
          .drop(CampaignMergedFields.EMAIL)
          .drop(CampaignMergedFields.NUMBER_SKUS)
          .drop(CampaignMergedFields.LIVE_CART_URL)
        
        val acartHourlyFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_ACART_HOURLY"
        DataWriter.writeCsv(campaignOutput, campaignType, campaignName, DataSets.HOURLY_MODE, TimeUtils.LAST_HOUR_FOLDER, acartHourlyFileName, DataSets.IGNORE_SAVEMODE, "true", ";")
      } else if (campaignName.equals(CampaignCommon.REPLENISHMENT_CAMPAIGN)) {
        DataWriter.writeParquet(campaignOutput, path, DataSets.IGNORE_SAVEMODE)
        val campaignCsv = campaignOutput
          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
          .drop(CampaignMergedFields.REC_SKUS)
          .drop(CampaignMergedFields.REF_SKUS)
          .drop(CampaignMergedFields.LIVE_MAIL_TYPE)
          .drop(CampaignMergedFields.EMAIL)
        //.drop(CampaignMergedFields.CUSTOMER_ID)

        val replenishFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_replenishment"
        DataWriter.writeCsv(campaignCsv, campaignType, campaignName, DataSets.DAILY_MODE, TimeUtils.YESTERDAY_FOLDER, replenishFileName, DataSets.IGNORE_SAVEMODE, "true", ";")

      } else {

        if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, path)) {
          DataWriter.writeParquet(campaignOutput, path, DataSets.IGNORE_SAVEMODE)
        }
      }

    }
  }

  def saveCampaignCSVForYesterday(campaignOutput: DataFrame, campaignName: String, fileName: String, campaignType: String = DataSets.PUSH_CAMPAIGNS) = {
    if (testMode) {
      saveTestData(campaignOutput, campaignName, campaignType)
    } else {
      val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
      DataWriter.writeCsv(campaignOutput, campaignType, campaignName, DataSets.DAILY_MODE, dateYesterday, fileName, DataSets.IGNORE_SAVEMODE, "true", ",")
    }
  }

  def saveTestData(campaignOutput: DataFrame, campaignName: String, campaignType: String): Unit = {
    if (null == testData) {
      testData = new mutable.MutableList[(DataFrame, String, String)]
    }
    testData += Tuple3(campaignOutput, campaignName, campaignType)
  }
}

