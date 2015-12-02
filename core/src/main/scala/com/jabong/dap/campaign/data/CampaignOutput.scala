package com.jabong.dap.campaign.data

import com.jabong.dap.campaign.manager.CampaignProcessor
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CampaignCommon }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.ContactListMobileVars
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable

object CampaignOutput {

  var testMode: Boolean = false
  var testData: mutable.MutableList[(DataFrame, String, String)] = null

  def setTestMode(newMode: Boolean): Unit = {
    testMode = newMode
    CampaignUtils.testMode = newMode
  }

  /**
   * save campaignsData
   * @param campaignOutput
   * @param campaignName
   * @param campaignType
   */
  def saveCampaignData(campaignOutput: DataFrame, campaignName: String, campaignType: String = DataSets.PUSH_CAMPAIGNS, incrDate: String) = {
    val path = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.DAILY_MODE, incrDate)

    if (testMode) {
      saveTestData(campaignOutput, campaignName, campaignType)
    } else {
      // not implemented old run for acart hourly
      if (campaignName.equals(CampaignCommon.ACART_HOURLY_CAMPAIGN)) {
        val dateToday = TimeUtils.getDateAfterHours(0, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)
        val acartPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, campaignName, DataSets.HOURLY_MODE, dateToday)
        if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, acartPath)) {
          DataWriter.writeParquet(campaignOutput, acartPath, DataSets.IGNORE_SAVEMODE)
        }
        //        val campaignCsvOutput = acartOutData
        //          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
        //          .drop(CampaignMergedFields.REC_SKUS)
        //          .drop(CampaignMergedFields.REF_SKUS)
        //          .drop(CampaignMergedFields.LIVE_MAIL_TYPE)
        //          .drop(CampaignMergedFields.EMAIL)
        //          .drop(CampaignMergedFields.NUMBER_SKUS)
        //          .drop(CampaignMergedFields.LIVE_CART_URL)

        //        val acartHourlyFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_ACART_HOURLY"
        //        DataWriter.writeCsv(campaignCsvOutput, campaignType, campaignName, DataSets.HOURLY_MODE, TimeUtils.CURRENT_HOUR_FOLDER, acartHourlyFileName, DataSets.IGNORE_SAVEMODE, "true", ";")
      } else if (campaignName.equals(CampaignCommon.REPLENISHMENT_CAMPAIGN)) {
        DataWriter.writeParquet(campaignOutput, path, DataSets.IGNORE_SAVEMODE)
        val campaignCsv = campaignOutput
          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
          .drop(CampaignMergedFields.REC_SKUS)
          .drop(CampaignMergedFields.REF_SKUS)
          .drop(CampaignMergedFields.LIVE_MAIL_TYPE)
          .drop(CampaignMergedFields.EMAIL)
        //.drop(CampaignMergedFields.CUSTOMER_ID)

        val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
        DataWriter.writeCsv(campaignCsv, campaignType, campaignName, DataSets.DAILY_MODE, incrDate, fileDate + "_replenishment", DataSets.IGNORE_SAVEMODE, "true", ";")
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

  def saveAcartHourlyFeed(inputData: DataFrame, cmr: DataFrame, acartHourlyFileName: String): DataFrame = {
    require(inputData != null, "acart hourly data cannot be null")
    val acartInputData = inputData.withColumn(CampaignCommon.PRIORITY, lit(""))
    val acartWithUidData = CampaignProcessor.mapEmailCampaignWithCMR(cmr, acartInputData)
    val inputDataNumberSkus = acartWithUidData.withColumn(CampaignMergedFields.NUMBER_SKUS, Udf.getAcartNumberOfSkus(col(CampaignMergedFields.LIVE_CART_URL)))
    val acartOutData = inputDataNumberSkus
      .withColumn(ContactListMobileVars.UID, lit(""))
      .withColumn(CampaignMergedFields.STATUS, lit(1))
      .withColumn(CampaignMergedFields.VISIT, lit(41))
      .withColumn(CampaignMergedFields.SPECIAL_TEXT, lit("4"))
      .withColumn(CampaignMergedFields.SENDING_METHOD, lit("4-N-N-N-N-N-N-N-N-41"))
      .withColumn(CampaignMergedFields.ARTICLE, when(col(CampaignMergedFields.NUMBER_SKUS) <= 4, lit(0)).otherwise(col(CampaignMergedFields.NUMBER_SKUS) - 4))
      .withColumn(CampaignMergedFields.URL, col(CampaignMergedFields.LIVE_CART_URL))
      .withColumn(CampaignMergedFields.ACART_REF_SKU + "1", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(0)))
      .withColumn(CampaignMergedFields.ACART_REF_SKU + "2", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(1), lit(0)))
      .withColumn(CampaignMergedFields.ACART_REF_SKU + "3", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(2), lit(0)))
      .withColumn(CampaignMergedFields.REC_SKU + "1", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(0)))
      .withColumn(CampaignMergedFields.REC_SKU + "2", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(1)))
      .withColumn(CampaignMergedFields.REC_SKU + "3", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(2)))
      .withColumn(CampaignMergedFields.REC_SKU + "4", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(3)))
      .withColumn(CampaignMergedFields.REC_SKU + "5", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(4)))
      .withColumn(CampaignMergedFields.REC_SKU + "6", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(5)))
      .withColumn(CampaignMergedFields.REC_SKU + "7", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(6)))
      .withColumn(CampaignMergedFields.REC_SKU + "8", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(7)))
      .drop(CampaignCommon.PRIORITY)

    CampaignUtils.debug(acartOutData, "AcartOutData")

    val campaignCsvOutput = acartOutData
      .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
      .drop(CampaignMergedFields.REC_SKUS)
      .drop(CampaignMergedFields.REF_SKUS)
      .drop(CampaignMergedFields.LIVE_MAIL_TYPE)
      .drop(CampaignMergedFields.EMAIL)
      .drop(CampaignMergedFields.NUMBER_SKUS)
      .drop(CampaignMergedFields.LIVE_CART_URL)
      .drop(CampaignMergedFields.CUSTOMER_ID)

    DataWriter.writeCsv(campaignCsvOutput, DataSets.EMAIL_CAMPAIGNS, CampaignCommon.ACART_HOURLY_CAMPAIGN, DataSets.HOURLY_MODE, TimeUtils.CURRENT_HOUR_FOLDER, acartHourlyFileName, DataSets.IGNORE_SAVEMODE, "true", ";")

    return acartOutData
  }
}

