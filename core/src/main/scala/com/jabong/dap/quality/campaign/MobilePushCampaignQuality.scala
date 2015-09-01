package com.jabong.dap.quality.campaign

import java.io.File

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.CampaignInfo
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * Created by raghu on 3/8/15.
 */
object MobilePushCampaignQuality extends Logging {
  val TRUE = true
  val zero: Long = 0
  val ANDROID = "Android"
  val IOS = "IOS"
  val WINDOWS = "Windows"
  val PRIORITYMERGE = "PriorityMerge"
  val TOTALCOUNT = "TotalCount"
  val CAMPAIGNNAME = "CampaignName"
  val CUSTID_ZERO = "CustIdZero"
  val CUSTID_NONZERO = "CustIdNonZero"

  val schema = StructType(Array(
    StructField(CAMPAIGNNAME, StringType, TRUE),
    StructField(TOTALCOUNT, LongType, TRUE),
    StructField(CUSTID_ZERO, LongType, TRUE),
    StructField(CUSTID_NONZERO, LongType, TRUE),
    StructField(PRIORITYMERGE, LongType, TRUE),
    StructField(ANDROID, LongType, TRUE),
    StructField(IOS, LongType, TRUE),
    StructField(WINDOWS, LongType, TRUE)
  ))

  def start(campaignsConfig: String) = {

    logger.info("Calling method start inside MobilePushCampaignQuality........")

    if (CampaignManager.initCampaignsConfigJson(campaignsConfig)) {

      val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
      val list: ListBuffer[Row] = new ListBuffer()

      list.appendAll(getCampaignQuality(CampaignCommon.MERGED_CAMPAIGN, dateYesterday))

      for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
        list.appendAll(getCampaignQuality(campaignDetails.campaignName, dateYesterday))
      }

      logger.info("Saving data frame of MOBILE_PUSH_CAMPAIGN_QUALITY........")

      val rdd = Spark.getContext().parallelize[Row](list.toSeq)
      val dfCampaignQuality = Spark.getSqlContext().createDataFrame(rdd, schema)

      val cachedfCampaignQuality = dfCampaignQuality.groupBy(CAMPAIGNNAME)
        .agg(sum(TOTALCOUNT) as TOTALCOUNT, sum(CUSTID_ZERO) as CUSTID_ZERO, sum(CUSTID_NONZERO) as CUSTID_NONZERO, sum(PRIORITYMERGE) as PRIORITYMERGE, sum(ANDROID) as ANDROID, sum(IOS) as IOS, sum(WINDOWS) as WINDOWS)
        .sort(CAMPAIGNNAME).cache()

      CampaignOutput.saveCampaignDataForYesterday(cachedfCampaignQuality, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY)

      DataWriter.writeCsv(cachedfCampaignQuality, DataSets.CAMPAIGNS, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY, DataSets.DAILY_MODE, dateYesterday, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY, DataSets.OVERWRITE_SAVEMODE, "true", ";")

      logger.info("MOBILE_PUSH_CAMPAIGN_QUALITY Data write successfully on this path :"
        + ConfigConstants.WRITE_OUTPUT_PATH + File.separator
        + DataSets.CAMPAIGNS + File.separator
        + CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY + File.separator
        + DataSets.DAILY_MODE + File.separator
        + dateYesterday
      )
    }

  }

  def getCampaignQuality(campaignName: String, dateYesterday: String): ListBuffer[Row] = {

    logger.info("Calling method getCampaignQuality........")

    val path = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, DataSets.CAMPAIGNS, campaignName, DataSets.DAILY_MODE, dateYesterday)

    val dataExits = DataVerifier.dataExists(path)

    var row: Row = null

    val list: ListBuffer[Row] = new ListBuffer()

    if (dataExits) {
      //if data frame is not null

      logger.info("Reading a Data Frame of: " + campaignName + " for Quality check")

      val dataFrame = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CAMPAIGNS, campaignName, DataSets.DAILY_MODE, dateYesterday)

      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {

        for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
          val campDF = dataFrame.filter("LIVE_MAIL_TYPE" + " = " + campaignDetails.mailType)
          val count = campDF.count()
          val countAndroid = campDF.filter(CampaignMergedFields.DOMAIN + " = '" + DataSets.ANDROID + "'").count()
          val countIos = campDF.filter(CampaignMergedFields.DOMAIN + " = '" + DataSets.IOS + "'").count()
          val countWindows = campDF.filter(CampaignMergedFields.DOMAIN + " = '" + DataSets.WINDOWS + "'").count()

          row = Row(campaignDetails.campaignName, zero, zero, zero, count, countAndroid, countIos, countWindows)
          list += row

        }

      } else {

        val countNonZeroFkCustomer = dataFrame.filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null").count()
        val countZeroFkCustomer = dataFrame.filter(CustomerVariables.FK_CUSTOMER + " = 0  or " + CustomerVariables.FK_CUSTOMER + " is null").count()

        row = Row(campaignName, dataFrame.count(), countZeroFkCustomer, countNonZeroFkCustomer, zero, zero, zero, zero)
        list += row
      }
    } else { //if data frame is null

      logger.info("Data Frame of: " + campaignName + " is null")

      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {
        for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
          row = Row(campaignDetails.campaignName, zero, zero, zero, zero, zero, zero, zero)
          list += row
        }
      } else {
        row = Row(campaignName, zero, zero, zero, zero, zero, zero, zero)
        list += row
      }
    }
    return list
  }
}