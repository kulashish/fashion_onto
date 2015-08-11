package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.campaignlist._
import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.{ CampaignConfig, CampaignInfo }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import scala.collection.mutable.HashMap

/**
 *  Campaign Manager will run multiple campaign based On Priority
 *  TODO: this class will need to be refactored to create a proper data flow of campaigns
 *
 */

object CampaignManager extends Serializable with Logging {

  // var campaignPriorityMap = new HashMap[String, Int]
  // var campaignMailTypeMap = new HashMap[String, Int]
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
        // campaignPriorityMap.put(campaignDetails.campaignName, campaignDetails.priority)
        // campaignMailTypeMap.put(campaignDetails.campaignName, campaignDetails.mailType)
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
    acartFollowup.runCampaign(past30DayCampaignMergedData, prev3rdDayAcartData, last3DaySalesOrderData, last3DaySalesOrderItemData, yesterdayItrData)

    // FIXME: part of customerselction for iod and lowstock can be merged

    // low stock - last 30 day acart (last30DayAcartData), yesterdayItrData, qty <=10
    //  yesterdayItrData
    // have not placed the order
    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)
    val acartLowStock = new AcartLowStockCampaign()
    acartLowStock.runCampaign(past30DayCampaignMergedData, last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, yesterdayItrData)

    // item on discount
    // last30DayAcartData
    // last30DaySalesOrderItemData = null  // created_at
    // last30DaySalesOrderData = null

    // itr last 30 days
    val last30daysItrData = CampaignInput.load30DayItrSkuSimpleData()

    val acartIOD = new AcartIODCampaign() //FIXME: RUN ACart Campaigns
    acartIOD.runCampaign(past30DayCampaignMergedData, last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, last30daysItrData)
  }

  //  val campaignPriority = udf((mailType: Int) => CampaignUtils.getCampaignPriority(mailType: Int, mailTypePriorityMap: scala.collection.mutable.HashMap[Int, Int]))

  def startWishlistCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    WishListCampaign.runCampaign()
  }

  def startSurfCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    SurfCampaign.runCampaign()

  }

  def initCampaignsConfig(campaignJsonPath: String) = {
    var json: JValue = null
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      //      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(campaignJsonPath)
      json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      // CampaignInfo.campaigns = json.extract[CampaignConfig]
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

  def initCampaignsConfigJson(campaignJsonPath: String): Boolean = {
    var json: JValue = null
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(campaignJsonPath)
      json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      CampaignInfo.campaigns = json.extract[CampaignConfig]
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
    return validated
  }

  /**
   * Merges all the campaign output based on priority
   * @param campaignJsonPath
   */
  def startPushCampaignMerge(campaignJsonPath: String) = {
    if (CampaignManager.initCampaignsConfigJson(campaignJsonPath)) {
      //      createCampaignMaps(json)
      val saveMode = DataSets.OVERWRITE_SAVEMODE
      val dateFolder = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
      val allCampaignsData = CampaignInput.loadAllCampaignsData(dateFolder)

      val cmr = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, dateFolder)
      val allCamp = CampaignProcessor.mapDeviceFromCMR(cmr, allCampaignsData)

      val itr = CampaignInput.loadYesterdayItrSkuDataForCampaignMerge()
      val mergedData = CampaignProcessor.mergepushCampaigns(allCamp, itr).coalesce(1).cache()

      println("Starting write parquet after repartitioning and caching")
      val writePath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.CAMPAIGN, CampaignCommon.MERGED_CAMPAIGN, DataSets.DAILY_MODE, dateFolder)
      if (DataWriter.canWrite(saveMode, writePath))
        DataWriter.writeParquet(mergedData, writePath, saveMode)

      //writing csv file
      CampaignProcessor.splitFileToCSV(mergedData, dateFolder)
    }
  }

}
