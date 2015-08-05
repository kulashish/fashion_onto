package com.jabong.dap.campaign.manager

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{ CustomerVariables, PageVisitVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.CampaignInfo
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Created by Mubarak on 28/7/15.
 */
object CampaignProcessor {

  val email = udf((s : String, s1: String) => if(null == s || s.equals("")) s1 else s)
  val device = udf((s : String, s1: String, s2: String) => if(s.contains("windows") || s.contains("android")| s.contains("ios")) s1 else s2)
  val domain = udf((s : String, s1: String) => if(s.contains("windows") || s.contains("android")| s.contains("ios")) s else s1)

  def mapDeviceFromCMR(cmr: DataFrame, campaign: DataFrame): DataFrame = {
    println("Starting the device mapping after dropping duplicates: ")// + campaign.count())

    val notNullCampaign = campaign.filter(!(col(CampaignMergedFields.CUSTOMER_ID) === 0 && col(CampaignMergedFields.DEVICE_ID) === ""))

    println("After dropping empty customer and device ids: ")// + notNullCampaign.count())

    println("Starting the CMR: ")// + cmr.count())
    //println("printing customer id = 0 records:")
    //cmr.filter(col(CustomerVariables.ID_CUSTOMER) === 0).show(10)

    //println("printing customer id = null records:")
    //cmr.filter(CustomerVariables.ID_CUSTOMER + " IS NULL").show(10)

    //println("printing device id = empty records:")
    //cmr.filter(col(PageVisitVariables.BROWSER_ID) === "").show(10)

    //println("printing device id = null records:")
    //cmr.filter(PageVisitVariables.BROWSER_ID + " IS NULL").show(10)

    val cmrn = cmr
      .filter(col(CustomerVariables.ID_CUSTOMER) > 0)
      .select(
        cmr(CustomerVariables.EMAIL),
        cmr(CustomerVariables.RESPONSYS_ID),
        cmr(CustomerVariables.ID_CUSTOMER),
        cmr(PageVisitVariables.BROWSER_ID),
        cmr(PageVisitVariables.DOMAIN)
      )

    println("After removing customer id = 0 or null: ")// + cmrn.count())

    val bcCampaign = Spark.getContext().broadcast(notNullCampaign).value
    val campaignDevice = cmrn.join(bcCampaign, bcCampaign(CampaignMergedFields.CUSTOMER_ID) === cmrn(CustomerVariables.ID_CUSTOMER), "rightouter")
      .select(
        bcCampaign(CampaignMergedFields.CUSTOMER_ID) as CampaignMergedFields.CUSTOMER_ID,
        bcCampaign(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        bcCampaign(CampaignMergedFields.REF_SKU1),
        bcCampaign(CampaignCommon.PRIORITY),
        /*  Other logic if UDF not required
        when(bcCampaign(CampaignMergedFields.DOMAIN).contains("windows") ||
          bcCampaign(CampaignMergedFields.DOMAIN).contains("android") ||
          bcCampaign(CampaignMergedFields.DOMAIN).contains("ios"), bcCampaign(CampaignMergedFields.DEVICE_ID).
        otherwise(cmrn(CampaignMergedFields.DEVICE_ID))
        ) as CampaignMergedFields.DEVICE_ID,
        when(bcCampaign(CampaignMergedFields.EMAIL) === null ||
          bcCampaign(CampaignMergedFields.EMAIL).equalTo(""),
          cmrn(CampaignMergedFields.EMAIL)).
          otherwise(bcCampaign(CampaignMergedFields.EMAIL)) as CampaignMergedFields.EMAIL,
        coalesce(bcCampaign(CampaignMergedFields.DOMAIN), cmrn(CampaignMergedFields.DOMAIN)) as CampaignMergedFields.DOMAIN
        */
        device(bcCampaign(CampaignMergedFields.DOMAIN), bcCampaign(CampaignMergedFields.DEVICE_ID), cmrn(PageVisitVariables.BROWSER_ID)) as CampaignMergedFields.DEVICE_ID,
        email(bcCampaign(CampaignMergedFields.EMAIL), cmrn(CampaignMergedFields.EMAIL)) as CampaignMergedFields.EMAIL,
        domain( bcCampaign(CampaignMergedFields.DOMAIN), cmrn(CampaignMergedFields.DOMAIN) as CampaignMergedFields.DOMAIN)
      )
    println("After joining campaigns with the cmr: " + campaignDevice.count())
    campaignDevice
  }

  def mergeCampaigns(campaign: DataFrame, itr: DataFrame): DataFrame = {
    println("Inside priority based merge")

    val custIdNotNUll = campaign.filter(!campaign(CampaignMergedFields.CUSTOMER_ID) === 0)
    println("After campaign filtering on not null CustomerId " + custIdNotNUll.count())
    //custIdNotNUll.printSchema()
    //custIdNotNUll.show(10)

    val custId = CampaignManager.campaignMerger(custIdNotNUll, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.DEVICE_ID)
    println("After campaign merger on CustomerId")
    //custId.printSchema()
    //custId.show(10)

    val custIdNUll = campaign.filter(campaign(CampaignMergedFields.CUSTOMER_ID) === 0 || campaign(CampaignMergedFields.CUSTOMER_ID) === 0)
    custIdNUll.show(100)
    println("After campaign filtering on null CustomerId " + custIdNUll.count())
    custIdNUll.printSchema()

    val DeviceId = CampaignManager.campaignMerger(custIdNUll, CampaignMergedFields.DEVICE_ID, CampaignMergedFields.CUSTOMER_ID)
    println("After campaign merger on DeviceId")
    DeviceId.printSchema()
    DeviceId.show(10)

    val camp = custId.unionAll(DeviceId)
    println("After unionAll count = " + camp.count())
    camp.printSchema()
    camp.show(10)

    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT) //YYYY-MM-DD

    val finalCampaign = camp.join(itr, camp(CampaignMergedFields.REF_SKU1) === itr(ITR.CONFIG_SKU))
      .select(
        camp(CampaignMergedFields.CUSTOMER_ID) as CampaignMergedFields.CUSTOMER_ID,
        camp(CampaignMergedFields.CAMPAIGN_MAIL_TYPE) as CampaignMergedFields.LIVE_MAIL_TYPE,
        camp(CampaignMergedFields.REF_SKU1) as CampaignMergedFields.LIVE_REF_SKU1,
        camp(CampaignMergedFields.EMAIL) as CampaignMergedFields.EMAIL,
        camp(CampaignMergedFields.DOMAIN) as CampaignMergedFields.DOMAIN,
        camp(CampaignMergedFields.DEVICE_ID) as CampaignMergedFields.deviceId,
        itr(ITR.PRODUCT_NAME) as CampaignMergedFields.LIVE_PROD_NAME,
        itr(ITR.BRAND_NAME) as CampaignMergedFields.LIVE_BRAND,
        itr(ITR.BRICK) as CampaignMergedFields.LIVE_BRICK,
        lit("www.jabong.com/cart/addmulti?skus=" + camp(CampaignMergedFields.REF_SKU1)).cast(StringType) as CampaignMergedFields.LIVE_CART_URL,
        lit(yesterdayDate).cast(StringType) as CampaignMergedFields.END_OF_DATE
      )
    println("Final Campaign after join with ITR: " + finalCampaign.count())
    finalCampaign.printSchema()
    finalCampaign.show(10)

    finalCampaign
  }

  def splitFileToCSV(df: DataFrame, date: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER), saveMode: String = DataSets.OVERWRITE_SAVEMODE) {
    val iosDF = df.filter(df(CampaignMergedFields.DOMAIN) === DataSets.IOS)
    val androidDF = df.filter(df(CampaignMergedFields.DOMAIN) === DataSets.ANDROID)

    exportCampaignCSV(iosDF, date, CampaignMergedFields.IOS_CODE, saveMode)
    exportCampaignCSV(androidDF, date, CampaignMergedFields.ANDROID_CODE, saveMode)

    for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
      val mailType = campaignDetails.mailType
      val iosSplitDF = iosDF.filter(CampaignMergedFields.LIVE_MAIL_TYPE + " = " + mailType).select(CampaignMergedFields.deviceId).distinct
      val androidSplitDF = androidDF.filter(CampaignMergedFields.LIVE_MAIL_TYPE + " = " + mailType).select(CampaignMergedFields.deviceId).distinct

      val fileI = campaignDetails.campaignName + mailType + "_" + CampaignMergedFields.IOS_CODE
      val fileA = campaignDetails.campaignName + mailType + "_" + CampaignMergedFields.ANDROID_CODE
      val filenameI = "staticlist_" + fileI + "_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      val filenameA = "staticlist_" + fileA + "_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

      DataWriter.writeCsv(iosSplitDF, DataSets.CAMPAIGN, fileI, DataSets.DAILY_MODE, date, filenameI, saveMode, "true", ";")
      DataWriter.writeCsv(androidSplitDF, DataSets.CAMPAIGN, fileA, DataSets.DAILY_MODE, date, filenameA, saveMode, "true", ";")
    }
  }

  def exportCampaignCSV(df: DataFrame, date: String = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER), domain: String, saveMode: String) {
    val dfResult = df.select(
      CampaignMergedFields.deviceId,
      CampaignMergedFields.LIVE_MAIL_TYPE,
      CampaignMergedFields.LIVE_BRAND,
      CampaignMergedFields.LIVE_REF_SKU1,
      CampaignMergedFields.LIVE_BRICK,
      CampaignMergedFields.LIVE_PROD_NAME,
      CampaignMergedFields.LIVE_CART_URL
    )
    val tablename =
      domain match {
        case CampaignMergedFields.IOS_CODE => DataSets.IOS
        case CampaignMergedFields.ANDROID_CODE => DataSets.ANDROID
      }

    val fileName = "updateDevices" + "_" + domain + "_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    //    println("writing to csv: " + dfResult.count())
    //    dfResult.printSchema()
    //    dfResult.show(10)

    //    val path = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, tablename, DataSets.DAILY_MODE, date)
    //    val csvFullPath = path + "/" + fileName

    DataWriter.writeCsv(dfResult, DataSets.CAMPAIGN, tablename, DataSets.DAILY_MODE, date, fileName, saveMode, "true", ";")
  }

}
