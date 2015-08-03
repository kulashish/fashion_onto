package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.campaignlist._
import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CampaignMergedFields}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.{CampaignConfig, CampaignInfo}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.HashMap

/**
 *  Campaign Manager will run multiple campaign based On Priority
 *  TODO: this class will need to be refactored to create a proper data flow of campaigns
 *
 */

object CampaignManager extends Serializable with Logging {

  var campaignPriorityMap = new HashMap[String, Int]
  var campaignMailTypeMap = new HashMap[String, Int]
  var mailTypePriorityMap = new HashMap[Int, Int]

  def createCampaignMaps(parsedJson: JValue): Boolean = {
    if (parsedJson == null) {
      return false
    }
    implicit val formats = net.liftweb.json.DefaultFormats
    try {
      CampaignInfo.campaigns = parsedJson.extract[CampaignConfig]
      //  var campaignDetails:CampaignDetail = null
      for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
        campaignPriorityMap.put(campaignDetails.campaignName, campaignDetails.priority)
        campaignMailTypeMap.put(campaignDetails.campaignName, campaignDetails.mailType)
        mailTypePriorityMap.put(campaignDetails.mailType, campaignDetails.priority)
      }

    } catch {
      case e: ParseException =>
        logger.error("cannot parse campaign Priority List")
        return false

      case e: NullPointerException =>
        logger.error("Null Pointer Exception")
        return false
    }

    return true
  }

  def startPushRetargetCampaign() = {
    val liveRetargetCampaign = new LiveRetargetCampaign()

    val orderItemData = CampaignInput.loadYesterdayOrderItemData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val orderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    liveRetargetCampaign.runCampaign(orderData, orderItemData)
  }

  def startPushInvalidCampaign(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)

    // invalid followup
    val fullOrderData = CampaignInput.loadFullOrderData()

    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()
    val orderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    // last 3 days of orderitem data
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val orderItemData = CampaignInput.loadLastNdaysOrderItemData(3, fullOrderItemData)

    // yesterday itr - Qty of Ref SKU to be greater than/equal to 10
    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData()

    val invalidFollowUp = new InvalidFollowUpCampaign()
    invalidFollowUp.runCampaign(past30DayCampaignMergedData, orderData, orderItemData, yesterdayItrData)

    // invalid lowstock
    // last 30 days of order item data
    val last30DayOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData)

    // last 2 months order data
    val last60DayOrderData = CampaignInput.loadLastNdaysOrderData(60, fullOrderData)

    val invalidLowStock = new InvalidLowStockCampaign()
    invalidLowStock.runCampaign(past30DayCampaignMergedData, last60DayOrderData, last30DayOrderItemData, yesterdayItrData)

  }

  def startPushAbandonedCartCampaign(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)

    // acart daily, acart followup, acart low stock, acart iod
    val last30DayAcartData = CampaignInput.loadLast30daysAcartData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData()
    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()

    // acart daily - last day acart data, ref sku not bought on last day
    // no previous campaign check
    // FIXME: search for email
    val yesterdayAcartData = CampaignInput.loadNthdayAcartData(1, last30DayAcartData)
    val yesterdaySalesOrderItemData = CampaignInput.loadYesterdayOrderItemData() // created_at
    val yesterdaySalesOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)
    val acartDaily = new AcartDailyCampaign()
    acartDaily.runCampaign(yesterdayAcartData, yesterdaySalesOrderData, yesterdaySalesOrderItemData, yesterdayItrData)

    // acart followup - only = 3rd days acart, still not bought ref skus, qty >= 10, yesterdayItrData
    val prev3rdDayAcartData = CampaignInput.loadNthdayAcartData(3, last30DayAcartData)
    val last3DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(3, fullOrderItemData) // created_at
    val last3DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(3, fullOrderData)

    val acartFollowup = new AcartFollowUpCampaign()
   // acartFollowup.runCampaign(past30DayCampaignMergedData, prev3rdDayAcartData, last3DaySalesOrderData, last3DaySalesOrderItemData, yesterdayItrData)

    // FIXME: part of customerselction for iod and lowstock can be merged

    // low stock - last 30 day acart (last30DayAcartData), yesterdayItrData, qty <=10
    //  yesterdayItrData
    // have not placed the order
    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)
    val acartLowStock = new AcartLowStockCampaign()
    //acartLowStock.runCampaign(past30DayCampaignMergedData, last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, yesterdayItrData)

    // item on discount
    // last30DayAcartData
    // last30DaySalesOrderItemData = null  // created_at
    // last30DaySalesOrderData = null

    // itr last 30 days
    val last30daysItrData = CampaignInput.load30DayItrSkuSimpleData()

    val acartIOD = new AcartIODCampaign() //FIXME: RUN ACart Campaigns
    //acartIOD.runCampaign(past30DayCampaignMergedData, last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, last30daysItrData)
  }

  val campaignPriority = udf((mailType: Int) => CampaignUtils.getCampaignPriority(mailType: Int, mailTypePriorityMap: scala.collection.mutable.HashMap[Int, Int]))

  def startWishlistCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    WishListCampaign.runCampaign()
  }

  def startSurfCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    SurfCampaign.runCampaign()

  }

  /**
   * takes union input of all campaigns and return merged campaign list
   * @param inputCampaignsData
   * @return
   */
  def campaignMerger(inputCampaignsData: DataFrame, key: String, key1: String): DataFrame = {
    if (inputCampaignsData == null) {
      logger.error("inputCampaignData is null")
      return null
    }

    if (!(inputCampaignsData.columns.contains(key) || inputCampaignsData.columns.contains(key1))) {
      logger.error("Keys doesn't Exists")
      return null
    }

    val campaignMerged = inputCampaignsData.orderBy(CampaignCommon.PRIORITY)
      .groupBy(key)
      .agg(first(CampaignMergedFields.CAMPAIGN_MAIL_TYPE) as (CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        first(CampaignCommon.PRIORITY) as (CampaignCommon.PRIORITY),
        first(CampaignMergedFields.REF_SKU1) as (CampaignMergedFields.REF_SKU1),
        first(key1) as key1,
        first(CampaignMergedFields.DOMAIN) as CampaignMergedFields.DOMAIN,
        first(CampaignMergedFields.EMAIL) as CampaignMergedFields.EMAIL)

    return campaignMerged
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
  
  def initCampaignsConfig(campaignJsonPath: String) = {
    var json: JValue = null
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(campaignJsonPath)
      json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      //   campaignInfo.campaigns = json.extract[campaignConfig]
      // COVarJsonValidator.validate(COVarJobConfig.coVarJobInfo)
      true
    } catch {
      case e: ParseException =>
        logger.error("Error while parsing JSON: " + e.getMessage)
        false

      case e: IllegalArgumentException =>
        logger.error("Error while validating JSON: " + e.getMessage)
        false

      case e: Exception =>
        logger.error("Some unknown error occurred: " + e.getMessage)
        throw e
        false
    }

    if (validated) {
      createCampaignMaps(json)
    }
  }
  
  
  
  /**
   * Merges all the campaign output based on priority
   * @param campaignJsonPath
   */
  def startPushCampaignMerge(campaignJsonPath: String) = {
    var json: JValue = null
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(campaignJsonPath)
      json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      //   campaignInfo.campaigns = json.extract[campaignConfig]
      // COVarJsonValidator.validate(COVarJobConfig.coVarJobInfo)
      true
    } catch {
      case e: ParseException =>
        logger.error("Error while parsing JSON: " + e.getMessage)
        false

      case e: IllegalArgumentException =>
        logger.error("Error while validating JSON: " + e.getMessage)
        false

      case e: Exception =>
        logger.error("Some unknown error occurred: " + e.getMessage)
        throw e
        false
    }

    if (validated) {
      createCampaignMaps(json)
      val saveMode = DataSets.OVERWRITE_SAVEMODE
      val dateFolder = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
      val allCampaignsData = CampaignInput.loadAllCampaignsData(dateFolder)

      val cmr = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, dateFolder)
      val allCamp = CampaignProcessor.mapDeviceFromCMR(cmr, allCampaignsData, CampaignMergedFields.CUSTOMER_ID)

      val itr = CampaignInput.loadYesterdayItrSkuDataForCampaignMerge()
      val mergedData = CampaignProcessor.splitNMergeCampaigns(allCamp, itr).repartition(1).cache()

      val writePath = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, CampaignCommon.MERGED_CAMPAIGN, DataSets.DAILY_MODE, dateFolder)
      if (DataWriter.canWrite(saveMode, writePath))
        DataWriter.writeParquet(mergedData, writePath, saveMode)

      //writing csv file
      splitFileToCSV(mergedData, dateFolder)
    }
  }
}
