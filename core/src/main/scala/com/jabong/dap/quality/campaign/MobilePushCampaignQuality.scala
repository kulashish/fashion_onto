package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 3/8/15.
 */
object MobilePushCampaignQuality extends Logging {

  val schema = StructType(Array(
    StructField("name", StringType, true),
    StructField("count", IntegerType, true)
  ))

  def startMobilePushCampaignQuality(campaignsConfig: String) = {

    CampaignManager.initCampaignsConfig(campaignsConfig)

    val dfCampaignQuality = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], schema)

    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.ACART_DAILY_CAMPAIGN, dateYesterday))

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.ACART_FOLLOWUP_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.ACART_IOD_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.ACART_LOWSTOCK_CAMPAIGN, dateYesterday))

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.INVALID_LOWSTOCK_CAMPAIGN, dateYesterday))

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.CANCEL_RETARGET_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.RETURN_RETARGET_CAMPAIGN, dateYesterday))

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.SURF1_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.SURF2_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.SURF3_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.SURF6_CAMPAIGN, dateYesterday))

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.WISHLIST_IOD_CAMPAIGN, dateYesterday))
    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN, dateYesterday))

    dfCampaignQuality.unionAll(getCampaignQuality(CampaignCommon.MERGED_CAMPAIGN, dateYesterday))

    CampaignOutput.saveCampaignDataForYesterday(dfCampaignQuality, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY)

  }

  def getCampaignQuality(campaignName: String, dateYesterday: String): DataFrame = {

    val path = PathBuilder.buildPath(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, campaignName, DataSets.DAILY_MODE, dateYesterday)

    val dataExits = DataVerifier.dataExists(path)

    var row: Row = null

    val dfCampaignQuality = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], schema)

    if (!dataExits) { //if data frame is not null

      logger.info("Reading a Data Frame of: " + campaignName + " for Quality check")

      val dataFrame = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3ProcessedVariable", DataSets.DAILY_MODE, dateYesterday)

      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {

        for (mailType <- CampaignManager.mailTypePriorityMap.keys) {

          val count = dataFrame.filter(CampaignManager.mailTypePriorityMap.get(mailType) + " = " + mailType).count()
          row = Row(campaignName + "_" + mailType, count)
          dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        }

      } else if (campaignName.equals(CampaignCommon.SURF1_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF2_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF3_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF6_CAMPAIGN)) {

        val countNonZeroFkCustomer = dataFrame.filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null")
        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_non_zero", countNonZeroFkCustomer)
        dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        val countZeroFkCustomer = dataFrame.filter(CustomerVariables.FK_CUSTOMER + " = 0  or " + CustomerVariables.FK_CUSTOMER + " is null")
        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_zero", countZeroFkCustomer)
        dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      } else {
        row = Row(campaignName, dataFrame.count())
        dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      }

    } else { //if data frame is null
      if (campaignName.equals(CampaignCommon.MERGED_CAMPAIGN)) {

        for (mailType <- CampaignManager.mailTypePriorityMap.keys) {
          row = Row(campaignName + "_" + mailType, 0)
          dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        }

      } else if (campaignName.equals(CampaignCommon.SURF1_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF2_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF3_CAMPAIGN)
        || campaignName.equals(CampaignCommon.SURF6_CAMPAIGN)) {

        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_non_zero", 0)
        dfCampaignQuality.unionAll(getDataFrameFromRow(row))

        row = Row(campaignName + "_" + CustomerVariables.FK_CUSTOMER + "_is_zero", 0)
        dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      } else {
        row = Row(campaignName, 0)
        dfCampaignQuality.unionAll(getDataFrameFromRow(row))
      }
    }

    return dfCampaignQuality

  }

  def getDataFrameFromRow(row: Row): DataFrame = {

    val rdd = Spark.getContext().parallelize[Row](Seq(row))

    return Spark.getSqlContext().createDataFrame(rdd, schema)
  }

}
