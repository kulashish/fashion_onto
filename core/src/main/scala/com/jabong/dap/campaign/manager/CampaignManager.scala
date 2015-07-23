package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.campaignlist.{ InvalidLowStockCampaign, InvalidFollowUpCampaign, LiveRetargetCampaign }
import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.acq.common.{ CampaignDetail, CampaignConfig, CampaignInfo }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.spark.SparkConf

import scala.collection.mutable.HashMap

/**
 *  Campaign Manager will run multiple campaign based On Priority
 *  TODO: this class will need to be refactored to create a proper data flow of campaigns
 *
 */
object CampaignManager extends Serializable with Logging {

  var campaignPriorityMap = new HashMap[String, Int]
  var campaignMailTypeMap = new HashMap[String, Int]
  //  def start(campaignJsonPath: String) = {
  //    val validated = try {
  //      val conf = new Configuration()
  //      val fileSystem = FileSystem.get(conf)
  //      implicit val formats = net.liftweb.json.DefaultFormats
  //      val path = new Path(campaignJsonPath)
  //      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
  //      campaignInfo.campaigns = json.extract[campaignConfig]
  //     // COVarJsonValidator.validate(COVarJobConfig.coVarJobInfo)
  //      true
  //    } catch {
  //      case e: ParseException =>
  //        logger.error("Error while parsing JSON: " + e.getMessage)
  //        false
  //
  //      case e: IllegalArgumentException =>
  //        logger.error("Error while validating JSON: " + e.getMessage)
  //        false
  //
  //      case e: Exception =>
  //        logger.error("Some unknown error occurred: " + e.getMessage)
  //        throw e
  //        false
  //    }
  //
  //    if (validated) {
  //      for (coVarJob <- COVarJobConfig.coVarJobInfo.coVar) {
  //        COVarJobConfig.coVarInfo = coVarJob
  //        //        coVarJob.source match {
  //        //          case "erp" | "bob" | "unicommerce" => new Merger().merge()
  //        //          case _ => logger.error("Unknown table source.")
  //        //        }
  //      }
  //    }

  def main(args: Array[String]) {
    val liveRetargetCampaign = new LiveRetargetCampaign()
    val conf = new SparkConf().setAppName("CampaignTest").set("spark.driver.allowMultipleContexts", "true")

    Spark.init(conf)
    val hiveContext = Spark.getHiveContext()
    val orderData = hiveContext.read.parquet(args(0))
    val orderItemData = hiveContext.read.parquet(args(1))
    liveRetargetCampaign.runCampaign(orderData, orderItemData)
  }

  def createCampaignMaps(json: String): Boolean = {
    if (json == null) {
      return false
    }
    implicit val formats = net.liftweb.json.DefaultFormats
    try {
      val parsedJson = parse(json)
      CampaignInfo.campaigns = parsedJson.extract[CampaignConfig]
      //  var campaignDetails:CampaignDetail = null
      for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {
        campaignPriorityMap.put(campaignDetails.campaignName, campaignDetails.priority)
        campaignMailTypeMap.put(campaignDetails.campaignName, campaignDetails.mailType)
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

  def startPushInvalidCampaign() = {
    // invalid followup
    val fullOrderData = CampaignInput.loadFullOrderData()

    val orderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    // last 3 days of orderitem data
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val orderItemData = CampaignInput.loadLastNdaysOrderItemData(3, fullOrderItemData)

    // yesterday itr - Qty of Ref SKU to be greater than/equal to 10
    // FIXME
    val yesterdayItrData = null

    val invalidFollowUp = new InvalidFollowUpCampaign()
    invalidFollowUp.runCampaign(orderData, orderItemData, yesterdayItrData)

    // invalid lowstock
    // last 30 days of order item data
    val last30DayOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData)

    // last 2 months order data
    val last60DayOrderData = CampaignInput.loadLastNdaysOrderData(60, fullOrderData)

    val invalidLowStock = new InvalidLowStockCampaign()
    invalidLowStock.runCampaign(last60DayOrderData, last30DayOrderItemData, yesterdayItrData)

  }

  def startPushCampaignMerge(json: String) = {

  }

  //
  //  def execute() = {
  //
  //    val liveRetargetCampaign = new LiveRetargetCampaign()
  //    liveRetargetCampaign.runCampaign(null)
  //
  //  }

}
