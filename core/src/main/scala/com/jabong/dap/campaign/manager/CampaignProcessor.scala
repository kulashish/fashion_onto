package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.{ GroupedUtils, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, PageVisitVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.CampaignInfo
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.OrderBySchema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ IntegerType, StringType }

/**
 * Created by Mubarak on 28/7/15.
 */
object CampaignProcessor {

  /**
   *
   * @param cmr
   * @param campaign
   * @return
   */
  def mapDeviceFromCMR(cmr: DataFrame, campaign: DataFrame): DataFrame = {
    println("Starting the device mapping after dropping duplicates: ") // + campaign.count())

    val notNullCampaign = campaign.filter(!(col(CampaignMergedFields.CUSTOMER_ID) === 0 && col(CampaignMergedFields.DEVICE_ID) === ""))

    println("After dropping empty customer and device ids: ") // + notNullCampaign.count())

    println("Starting the CMR: ") // + cmr.count())
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
        cmr(CustomerVariables.ID_CUSTOMER),
        cmr(PageVisitVariables.BROWSER_ID),
        cmr(PageVisitVariables.DOMAIN)
      )

    println("After removing customer id = 0 or null ") // + cmrn.count())

    val bcCampaign = Spark.getContext().broadcast(notNullCampaign).value
    val campaignDevice = cmrn.join(bcCampaign, bcCampaign(CampaignMergedFields.CUSTOMER_ID) === cmrn(CustomerVariables.ID_CUSTOMER), SQL.RIGHT_OUTER)
      .select(
        bcCampaign(CampaignMergedFields.CUSTOMER_ID) as CampaignMergedFields.CUSTOMER_ID,
        bcCampaign(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        bcCampaign(CampaignMergedFields.REF_SKU1),
        bcCampaign(CampaignCommon.PRIORITY),
        /*  Other logic if UDF not required
        when(bcCampaign(CampaignMergedFields.DOMAIN).contains(DataSets.WINDOWS) ||
          bcCampaign(CampaignMergedFields.DOMAIN).contains(DataSets.ANDROID) ||
          bcCampaign(CampaignMergedFields.DOMAIN).contains(DataSets.IOS), bcCampaign(CampaignMergedFields.DEVICE_ID).
        otherwise(cmrn(CampaignMergedFields.DEVICE_ID))
        ) as CampaignMergedFields.DEVICE_ID,
        when(bcCampaign(CampaignMergedFields.EMAIL) === null ||
          bcCampaign(CampaignMergedFields.EMAIL).equalTo(""),
          cmrn(CampaignMergedFields.EMAIL)).
          otherwise(bcCampaign(CampaignMergedFields.EMAIL)) as CampaignMergedFields.EMAIL,
        coalesce(bcCampaign(CampaignMergedFields.DOMAIN), cmrn(CampaignMergedFields.DOMAIN)) as CampaignMergedFields.DOMAIN
        */
        Udf.device(bcCampaign(CampaignMergedFields.DOMAIN), bcCampaign(CampaignMergedFields.DEVICE_ID), cmrn(PageVisitVariables.BROWSER_ID)) as CampaignMergedFields.DEVICE_ID,
        Udf.email(bcCampaign(CampaignMergedFields.EMAIL), cmrn(CampaignMergedFields.EMAIL)) as CampaignMergedFields.EMAIL,
        Udf.domain(bcCampaign(CampaignMergedFields.DOMAIN), cmrn(CampaignMergedFields.DOMAIN)) as CampaignMergedFields.DOMAIN
      )
    println("After joining campaigns with the cmr: " + campaignDevice.count())
    campaignDevice
  }

  /**
   * Gets customer id  from customer master data for merged email campaigns
   * @param cmr
   * @param campaign
   * @return
   */
  def mapEmailCampaignWithCMR(cmr: DataFrame, campaign: DataFrame): DataFrame = {
    println("Starting the device mapping after dropping duplicates: ") // + campaign.count())

    val cmrn = cmr
      .filter(col(CustomerVariables.ID_CUSTOMER) > 0)
      .select(
        cmr(ContactListMobileVars.UID),
        cmr(CustomerVariables.EMAIL),
        cmr(CustomerVariables.ID_CUSTOMER),
        cmr(PageVisitVariables.BROWSER_ID),
        cmr(PageVisitVariables.DOMAIN)
      )

    println("After removing customer id = 0 or null ") // + cmrn.count())

    val bcCampaign = Spark.getContext().broadcast(campaign).value
    val emailData = cmrn.join(bcCampaign, bcCampaign(CustomerVariables.EMAIL) === cmrn(CustomerVariables.EMAIL), SQL.INNER)
      .select(
        cmrn(ContactListMobileVars.UID),
        cmrn(CustomerVariables.ID_CUSTOMER) as CampaignMergedFields.CUSTOMER_ID,
        bcCampaign(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        bcCampaign(CampaignMergedFields.REF_SKUS),
        bcCampaign(CampaignMergedFields.REC_SKUS),
        bcCampaign(CampaignCommon.PRIORITY),
        bcCampaign(CampaignMergedFields.LIVE_CART_URL),
        bcCampaign(CampaignMergedFields.EMAIL) as CampaignMergedFields.EMAIL
      )

    CampaignUtils.debug(emailData, "data after join with cmr in campaign Processor")
    emailData
  }
  /**
   * takes union input of all campaigns and return merged campaign list
   * @param inputCampaignsData
   * @param groupKey
   * @return
   */
  def campaignMerger(inputCampaignsData: DataFrame, groupKey: String): DataFrame = {
    if (inputCampaignsData == null) {
      // logger.error("inputCampaignData is null")
      return null
    }

    if (!(inputCampaignsData.columns.contains(groupKey))) {
      // logger.error("Keys doesn't Exists")
      return null
    }

    val groupedFields = Array(groupKey)
    val aggFields = Array(groupKey, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.REF_SKU1, CampaignMergedFields.EMAIL, CampaignMergedFields.DOMAIN)

    val campaignMerged = GroupedUtils.orderGroupBy(inputCampaignsData, groupedFields, aggFields, GroupedUtils.FIRST, OrderBySchema.pushCampaignSchema, CampaignCommon.PRIORITY, GroupedUtils.ASC, IntegerType)

    campaignMerged
  }

  /**
   * Priority based merge for push campaigns.
   * @param allCampaign
   * @param itr
   * @return
   */
  def mergePushCampaigns(allCampaign: DataFrame, itr: DataFrame): DataFrame = {
    println("Inside priority based merge")

    // filtering based on domain as this is only for push campaigns and only for ios and android. Windows is also not needed.
    val campaign = allCampaign.filter(CampaignMergedFields.DOMAIN + " IN ('" + DataSets.IOS + "', '" + DataSets.ANDROID + "')")

    // removing as this is not needed in case of ad4push campaigns. We are getting multiple customers with same deviceIds
    // with the below logic.
    // val custIdNotNUll = campaign.filter(!(campaign(CampaignMergedFields.CUSTOMER_ID) === 0))
    // println("After campaign filtering on not null CustomerId ") // + custIdNotNUll.count())
    // //custIdNotNUll.printSchema()
    // //custIdNotNUll.show(10)
    //
    // val custId = campaignMerger(custIdNotNUll, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.DEVICE_ID)
    // println("After campaign merger on CustomerId")
    // //custId.printSchema()
    // //custId.show(10)
    //
    // val custIdNUll = campaign.filter(campaign(CampaignMergedFields.CUSTOMER_ID) === 0)
    // println("After campaign filtering on null CustomerId ") // + custIdNUll.count())
    // //custIdNUll.printSchema()
    // //custIdNUll.show(10)
    //
    // val DeviceId = campaignMerger(custIdNUll, CampaignMergedFields.DEVICE_ID, CampaignMergedFields.CUSTOMER_ID)
    // println("After campaign merger on DeviceId")
    // //DeviceId.printSchema()
    // //DeviceId.show(10)
    //
    // val camp = custId.unionAll(DeviceId)
    // println("After unionAll ") // + camp.count())
    // //camp.printSchema()
    // //camp.show(10)

    val camp = campaignMerger(campaign, CampaignMergedFields.DEVICE_ID)
    println("After campaign merger on DeviceId")
    //DeviceId.printSchema()
    //DeviceId.show(10)

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
        lit("").cast(StringType) as CampaignMergedFields.LIVE_CART_URL,
        lit(yesterdayDate).cast(StringType) as CampaignMergedFields.END_OF_DATE
      )
    println("Final Campaign after join with ITR ") // + finalCampaign.count())
    //finalCampaign.printSchema()
    //finalCampaign.show(10)

    finalCampaign
  }

  /**
   * Merge EmailCampaigns
   * @param allCampaignsData
   * @return
   */
  def mergeEmailCampaign(allCampaignsData: DataFrame): DataFrame = {

    val groupedFields = Array(CampaignMergedFields.EMAIL)
    val aggFields = Array(CampaignMergedFields.EMAIL, ContactListMobileVars.UID, CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.REF_SKUS, CampaignMergedFields.REC_SKUS, CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.LIVE_CART_URL)

    val campaignMerged = GroupedUtils.orderGroupBy(allCampaignsData, groupedFields, aggFields, GroupedUtils.FIRST, OrderBySchema.emailCampaignSchema, CampaignCommon.PRIORITY, GroupedUtils.ASC, IntegerType)

    CampaignUtils.debug(campaignMerged, "data after campaign merge in campaign Processor")

    campaignMerged
  }
  /**
   *
   * @param iosDF
   * @param date
   * @param saveMode
   */
  def splitFileToCSV(iosDF: DataFrame, androidDF: DataFrame, date: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER), saveMode: String = DataSets.OVERWRITE_SAVEMODE) {
    exportCampaignCSV(iosDF, date, DataSets.IOS_CODE, saveMode)
    exportCampaignCSV(androidDF, date, DataSets.ANDROID_CODE, saveMode)

    for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
      val mailType = campaignDetails.mailType
      val iosSplitDF = iosDF.filter(CampaignMergedFields.LIVE_MAIL_TYPE + " = " + mailType).select(CampaignMergedFields.deviceId).distinct
      val androidSplitDF = androidDF.filter(CampaignMergedFields.LIVE_MAIL_TYPE + " = " + mailType).select(androidDF(PageVisitVariables.ADD4PUSH) as CampaignMergedFields.deviceId).distinct

      val fileI = campaignDetails.campaignName + mailType + "_" + DataSets.IOS_CODE
      val fileA = campaignDetails.campaignName + mailType + "_" + DataSets.ANDROID_CODE
      val filenameI = "staticlist_" + fileI + "_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      val filenameA = "staticlist_" + fileA + "_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

      DataWriter.writeCsv(iosSplitDF, DataSets.PUSH_CAMPAIGNS, fileI, DataSets.DAILY_MODE, date, filenameI, saveMode, "true", ";", 1)
      DataWriter.writeCsv(androidSplitDF, DataSets.PUSH_CAMPAIGNS, fileA, DataSets.DAILY_MODE, date, filenameA, saveMode, "true", ";", 1)
    }
  }

  /**
   *
   * @param df
   * @param date
   * @param domain
   * @param saveMode
   */
  def exportCampaignCSV(df: DataFrame, date: String = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER), domain: String, saveMode: String) {
    val dfResult = df.select(
      when(df(CampaignMergedFields.DOMAIN) === DataSets.ANDROID, df(PageVisitVariables.ADD4PUSH)).otherwise(df(CampaignMergedFields.deviceId)) as CampaignMergedFields.deviceId,
      df(CampaignMergedFields.LIVE_MAIL_TYPE),
      df(CampaignMergedFields.LIVE_BRAND),
      df(CampaignMergedFields.LIVE_REF_SKU1),
      df(CampaignMergedFields.LIVE_BRICK),
      df(CampaignMergedFields.LIVE_PROD_NAME),
      df(CampaignMergedFields.LIVE_CART_URL)
    )
    val tablename =
      domain match {
        case DataSets.IOS_CODE => DataSets.IOS
        case DataSets.ANDROID_CODE => DataSets.ANDROID
      }

    val fileName = "updateDevices" + "_" + domain + "_" + TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    //    println("writing to csv: " + dfResult.count())
    //    dfResult.printSchema()
    //    dfResult.show(10)

    DataWriter.writeCsv(dfResult, DataSets.PUSH_CAMPAIGNS, tablename, DataSets.DAILY_MODE, date, fileName, saveMode, "true", ";", 1)
  }

  def addAd4pushId(ad4push: DataFrame, campaigns: DataFrame): DataFrame = {
    val ad4pushBc = Spark.getContext().broadcast(ad4push).value
    val joined = campaigns.join(ad4pushBc, ad4pushBc(PageVisitVariables.BROWSER_ID) === campaigns(CampaignMergedFields.deviceId), SQL.LEFT_OUTER)
      .select(campaigns(CampaignMergedFields.CUSTOMER_ID),
        campaigns(CampaignMergedFields.LIVE_MAIL_TYPE),
        campaigns(CampaignMergedFields.LIVE_REF_SKU1),
        campaigns(CampaignMergedFields.EMAIL),
        campaigns(CampaignMergedFields.DOMAIN),
        ad4push(PageVisitVariables.ADD4PUSH) as PageVisitVariables.ADD4PUSH,
        campaigns(CampaignMergedFields.deviceId),
        campaigns(CampaignMergedFields.LIVE_PROD_NAME),
        campaigns(CampaignMergedFields.LIVE_BRAND),
        campaigns(CampaignMergedFields.LIVE_BRICK),
        campaigns(CampaignMergedFields.LIVE_CART_URL),
        campaigns(CampaignMergedFields.END_OF_DATE))

    return joined
  }

}
