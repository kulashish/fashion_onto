package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CampaignCommon }
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.CampaignInfo
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 3/8/15.
 */
object MobilePushCampaignQuality extends Logging {

  val schema = StructType(Array(
    StructField("name", StringType, true),
    StructField("count", LongType, true)
  ))

  def startMobilePushCampaignQuality(campaignsConfig: String) = {

    logger.info("Calling method startMobilePushCampaignQuality........")

    if (CampaignManager.initCampaignsConfigJson(campaignsConfig)) {

      val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

      var dfCampaignQuality = getCampaignQuality(CampaignCommon.MERGED_CAMPAIGN, dateYesterday)

      for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {

        dfCampaignQuality = dfCampaignQuality.unionAll(getCampaignQuality(campaignDetails.campaignName, dateYesterday))

      }

      logger.info("Saving data frame of MOBILE_PUSH_CAMPAIGN_QUALITY........")

      val cachedfCampaignQuality = dfCampaignQuality.cache()

      CampaignOutput.saveCampaignDataForYesterday(cachedfCampaignQuality, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY)

      DataWriter.writeCsv(cachedfCampaignQuality, DataSets.CAMPAIGN, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY + "_csv", DataSets.DAILY_MODE, dateYesterday, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY, DataSets.OVERWRITE_SAVEMODE, "true", ";")

      logger.info("MOBILE_PUSH_CAMPAIGN_QUALITY Data write successfully on this path :"
        + DataSets.OUTPUT_PATH + "/"
        + DataSets.CAMPAIGN + "/"
        + CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY + "/"
        + DataSets.DAILY_MODE + "/"
        + dateYesterday
      )
    }

  }

  def getCampaignQuality(campaignName: String, dateYesterday: String): DataFrame = {

    logger.info("Calling method getCampaignQuality........")

    val path = PathBuilder.buildPath(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, campaignName, DataSets.DAILY_MODE, dateYesterday)

    val dataExits = DataVerifier.dataExists(path)

    var row: Row = null

    var dfCampaignQuality = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], schema)

    if (dataExits) { //if data frame is not null

      logger.info("Reading a Data Frame of: " + campaignName + " for Quality check")

      val dataFrame = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, campaignName, DataSets.DAILY_MODE, dateYesterday)

      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {

        //        for (mailType <- CampaignManager.mailTypePriorityMap.keys) {
        for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {

          val count = dataFrame.filter("LIVE_MAIL_TYPE" + " = " + campaignDetails.mailType).count()
          row = Row(campaignName + "_" + campaignDetails.campaignName, count)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

          val countAndroid = dataFrame.filter(CampaignMergedFields.DOMAIN + " = '" + DataSets.ANDROID + "'").count()
          row = Row(campaignDetails.campaignName + "_" + DataSets.ANDROID, countAndroid)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

          val countIos = dataFrame.filter(CampaignMergedFields.DOMAIN + " = '" + DataSets.IOS + "'").count()
          row = Row(campaignDetails.campaignName + "_" + DataSets.IOS, countIos)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

          val countWindows = dataFrame.filter(CampaignMergedFields.DOMAIN + " = '" + DataSets.WINDOWS + "'").count()
          row = Row(campaignDetails.campaignName + "_" + DataSets.WINDOWS, countWindows)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        }

      } else if (campaignName.equals(CampaignCommon.SURF1_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF2_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF3_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF6_CAMPAIGN)) {

        val countNonZeroFkCustomer = dataFrame.filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null").count()
        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_non_zero", countNonZeroFkCustomer)
        dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        val countZeroFkCustomer = dataFrame.filter(CustomerVariables.FK_CUSTOMER + " = 0  or " + CustomerVariables.FK_CUSTOMER + " is null").count()
        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_zero", countZeroFkCustomer)
        dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      } else {
        row = Row(campaignName, dataFrame.count())
        dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      }

    } else { //if data frame is null

      val count: Long = 0

      logger.info("Data Frame of: " + campaignName + " is null")

      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {

        //        for (mailType <- CampaignManager.mailTypePriorityMap.keys) {
        for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {

          row = Row(campaignName + "_" + campaignDetails.campaignName, count)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

          row = Row(campaignDetails.campaignName + "_" + DataSets.ANDROID, count)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

          row = Row(campaignDetails.campaignName + "_" + DataSets.IOS, count)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

          row = Row(campaignDetails.campaignName + "_" + DataSets.WINDOWS, count)
          dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        }

      } else if (campaignName.equals(CampaignCommon.SURF1_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF2_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF3_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF6_CAMPAIGN)) {

        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_non_zero", count)
        dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_zero", count)
        dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      } else {
        row = Row(campaignName, count)
        dfCampaignQuality = dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      }
    }

    return dfCampaignQuality

  }

  def getDataFrameFromRow(row: Row): DataFrame = {

    val rdd = Spark.getContext().parallelize[Row](Seq(row))

    return Spark.getSqlContext().createDataFrame(rdd, schema)
  }

}