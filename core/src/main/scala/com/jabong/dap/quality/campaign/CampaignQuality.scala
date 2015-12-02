package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables }
import com.jabong.dap.common.mail.ScalaMail
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.{ CampaignInfo, DbConnection }
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SaveMode }

import scala.collection.mutable.ListBuffer

/**
 * Created by raghu on 3/8/15.
 */
object CampaignQuality extends Logging {
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

  val EMAIL_NULL = "EmailNull"
  val EMAIL_NON_NULL = "EmailNonNull"

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

  def start(campaignsConfig: String, campaignType: String) = {

    logger.info("Calling method start inside CampaignQuality........")

    if (CampaignManager.initCampaignsConfigJson(campaignsConfig)) {

      val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
      val list: ListBuffer[Row] = new ListBuffer()

      list.appendAll(getCampaignQuality(CampaignCommon.MERGED_CAMPAIGN, dateYesterday, campaignType))

      for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
        list.appendAll(getCampaignQuality(campaignDetails.campaignName, dateYesterday, campaignType))
      }

      val rdd = Spark.getContext().parallelize[Row](list.toSeq)
      val dfCampaignQuality = Spark.getSqlContext().createDataFrame(rdd, schema)

      val cachedfCampaignQuality = dfCampaignQuality.groupBy(CAMPAIGNNAME)
        .agg(sum(TOTALCOUNT) as TOTALCOUNT, sum(CUSTID_ZERO) as CUSTID_ZERO, sum(CUSTID_NONZERO) as CUSTID_NONZERO, sum(PRIORITYMERGE) as PRIORITYMERGE, sum(ANDROID) as ANDROID, sum(IOS) as IOS, sum(WINDOWS) as WINDOWS)
        .sort(CAMPAIGNNAME).cache()

      if (campaignType.equals(DataSets.PUSH_CAMPAIGNS)) {

        sendMail(cachedfCampaignQuality, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY, dateYesterday)
        writeForJDaRe(cachedfCampaignQuality.withColumn("date", lit(TimeUtils.changeDateFormat(dateYesterday, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT))), CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY)

      } else {

        val df = campaignType match {
          case DataSets.VARIABLES => {
            cachedfCampaignQuality.select(
              col(CAMPAIGNNAME) as "VariablesName",
              col(TOTALCOUNT),
              col(CUSTID_ZERO) as "NullCount",
              col(CUSTID_NONZERO) as "NotNullCount"
            )
          }
          case _ => {
            cachedfCampaignQuality.select(
              col(CAMPAIGNNAME),
              col(TOTALCOUNT),
              col(CUSTID_ZERO) as EMAIL_NULL,
              col(CUSTID_NONZERO) as EMAIL_NON_NULL,
              col(PRIORITYMERGE)
            )
          }
        }

        sendMail(df, campaignType, dateYesterday)
        writeForJDaRe(df.withColumn("date", lit(TimeUtils.changeDateFormat(dateYesterday, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT))), DataSets.CALENDAR_CAMPAIGNS + "_QUALITY")

      }

    }

  }

  def sendMail(df: DataFrame, campaignType: String, date: String) = {

    CampaignOutput.saveCampaignDataForYesterday(df, campaignType)

    //    DataWriter.writeCsv(df, DataSets.CAMPAIGNS, campaignType, DataSets.DAILY_MODE, date, campaignType, DataSets.OVERWRITE_SAVEMODE, "true", ";")

    val emailSubscribers = OptionUtils.getOptValue(CampaignInfo.campaigns.emailSubscribers, "tech.dap@jabong.com")

    val content = ScalaMail.generateHTML(df)

    ScalaMail.sendMessage("tech.dap@jabong.com", emailSubscribers, "", "tech.dap@jabong.com", campaignType, content)

  }

  def writeForJDaRe(df: DataFrame, campaignType: String) = {
    val dbConn = new DbConnection(CampaignCommon.J_DARE_SOURCE)
    val dateNow = new java.util.Date
    val tsString = new java.sql.Timestamp(dateNow.getTime).toString
    val dfWithInsertedOn = df.withColumn("created_at", lit(tsString))
    dfWithInsertedOn.write.mode(SaveMode.Append).jdbc(dbConn.getConnectionString, campaignType, dbConn.getConnectionProperties)
  }

  def getCampaignQuality(campaignName: String, dateYesterday: String, campaignType: String): ListBuffer[Row] = {

    logger.info("Calling method getCampaignQuality........")

    val path = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, campaignType, campaignName, DataSets.DAILY_MODE, dateYesterday)

    val dataExits = DataVerifier.dataExists(path)

    var row: Row = null

    val list: ListBuffer[Row] = new ListBuffer()

    if (dataExits) {
      //if data frame is not null

      logger.info("Reading a Data Frame of: " + campaignName + " for Quality check")

      val dataFrame = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, campaignType, campaignName, DataSets.DAILY_MODE, dateYesterday)

      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {

        for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {

          val campDF = {
            //this filter for Calendar Campaign Quality
            if (dataFrame.schema.fieldNames.contains(CampaignMergedFields.CALENDAR_MAIL_TYPE)) {
              dataFrame.filter(col(CampaignMergedFields.CALENDAR_MAIL_TYPE) === campaignDetails.mailType)
            } else if (dataFrame.schema.fieldNames.contains(CampaignMergedFields.LIVE_MAIL_TYPE)) { //this filter for Email/Push Campaign Quality
              dataFrame.filter(col(CampaignMergedFields.LIVE_MAIL_TYPE) === campaignDetails.mailType)
            } else {
              dataFrame
            }
          }

          val count = campDF.count()

          if (campaignType.equals(DataSets.PUSH_CAMPAIGNS)) {
            val countAndroid = campDF.filter(col(CampaignMergedFields.DOMAIN) === DataSets.ANDROID).count()
            val countIos = campDF.filter(col(CampaignMergedFields.DOMAIN) === DataSets.IOS).count()
            val countWindows = campDF.filter(col(CampaignMergedFields.DOMAIN) === DataSets.WINDOWS).count()
            row = Row(campaignDetails.campaignName, zero, zero, zero, count, countAndroid, countIos, countWindows)
          } else {
            row = Row(campaignDetails.campaignName, zero, zero, zero, count, zero, zero, zero)
          }
          list += row

        }

      } else {

        if (campaignType.equals(DataSets.PUSH_CAMPAIGNS)) {
          val countZeroFkCustomer = dataFrame.filter(col(CustomerVariables.FK_CUSTOMER).isNull || col(CustomerVariables.FK_CUSTOMER).leq(0)).count()
          val countNonZeroFkCustomer = dataFrame.filter(col(CustomerVariables.FK_CUSTOMER).isNotNull && col(CustomerVariables.FK_CUSTOMER).gt(0)).count()
          row = Row(campaignName, dataFrame.count(), countZeroFkCustomer, countNonZeroFkCustomer, zero, zero, zero, zero)
        } else if (campaignType.equals(DataSets.EMAIL_CAMPAIGNS) || campaignType.equals(DataSets.CALENDAR_CAMPAIGNS)) {

          val countNullEmail = dataFrame.filter(col(CustomerVariables.EMAIL).isNull).count()
          val countNonNullEmail = dataFrame.filter(col(CustomerVariables.EMAIL).isNotNull).count()
          row = Row(campaignName, dataFrame.count(), countNullEmail, countNonNullEmail, zero, zero, zero, zero)
        } else {

          var countNull: Long = 0
          var countNotNull: Long = 0
          val fieldNames = dataFrame.schema.fieldNames
          if (fieldNames.contains(CustomerVariables.EMAIL)) {
            countNull = dataFrame.filter(col(CustomerVariables.EMAIL).isNull).count()
            countNotNull = dataFrame.filter(col(CustomerVariables.EMAIL).isNotNull).count()
          } else if (fieldNames.contains(CustomerVariables.CUSTOMER_ID)) {
            countNull = dataFrame.filter(col(CustomerVariables.CUSTOMER_ID).isNull).count()
            countNotNull = dataFrame.filter(col(CustomerVariables.CUSTOMER_ID).isNotNull).count()
          } else if (fieldNames.contains(ContactListMobileVars.UID)) {
            countNull = dataFrame.filter(col(ContactListMobileVars.UID).isNull).count()
            countNotNull = dataFrame.filter(col(ContactListMobileVars.UID).isNotNull).count()
          } else if (fieldNames.contains(CustomerVariables.FK_CUSTOMER)) {
            countNull = dataFrame.filter(col(CustomerVariables.FK_CUSTOMER).isNull || col(CustomerVariables.FK_CUSTOMER).leq(0)).count()
            countNotNull = dataFrame.filter(col(CustomerVariables.FK_CUSTOMER).isNotNull && col(CustomerVariables.FK_CUSTOMER).gt(0)).count()
          } else {
            countNotNull = dataFrame.count()
          }

          row = Row(campaignName, dataFrame.count(), countNull, countNotNull, zero, zero, zero, zero)
        }

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