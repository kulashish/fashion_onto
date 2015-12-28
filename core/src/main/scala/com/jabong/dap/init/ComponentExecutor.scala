package com.jabong.dap.init

import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.campaign.recommendation.generator.RecommendationGenerator
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.data.acq.common._
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.export.SkuData
import com.jabong.dap.export.dcf.DcfFeedGenerator
import com.jabong.dap.export.mongo.MongoFeedGenerator
import com.jabong.dap.model.ad4push.data.Ad4pushDeviceMerger
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import com.jabong.dap.model.city.CityData
import com.jabong.dap.model.clickstream.campaignData.CustomerAppDetails
import com.jabong.dap.model.clickstream.variables.{ GetSurfVariables, SurfVariablesMain }
import com.jabong.dap.model.customer.campaigndata._
import com.jabong.dap.model.customer.data._
import com.jabong.dap.model.order.variables.{ SalesItemRevenue, SalesOrderAddress }
import com.jabong.dap.model.product.itr.BasicITR
import com.jabong.dap.model.responsys.campaigndata.CustomerPreferredTimeslotPart1
import com.jabong.dap.quality.Clickstream.DataQualityMethods
import com.jabong.dap.quality.campaign.CampaignQualityEntry
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Created by pooja on 9/7/15.
 */
class ComponentExecutor extends Serializable with Logging {

  @throws(classOf[Exception])
  def start(paramJsonPath: String) = {
    val validated = try {
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      implicit val formats = net.liftweb.json.DefaultFormats
      val path = new Path(paramJsonPath)
      val json = parse(scala.io.Source.fromInputStream(fileSystem.open(path)).mkString)
      ParamJobConfig.paramJobInfo = json.extract[ParamJobInfo]
      ParamJsonValidator.validate(ParamJobConfig.paramJobInfo)
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
      val isHistory = OptionUtils.getOptBoolVal(ParamJobConfig.paramJobInfo.isHistory)

      println("isHistory: " + isHistory)
      println("ParamJobConfig.paramJobInfo:" + ParamJobConfig.paramJobInfo)

      for (paramJob <- ParamJobConfig.paramJobInfo.params) {
        ParamJobConfig.paramInfo = paramJob
        paramJob.source match {
          // Basic ITR
          case DataSets.BASIC_ITR => BasicITR.start(paramJob, isHistory)

          // Customer Master Record
          case DataSets.CUSTOMER_DEVICE_MAPPING => CustomerDeviceMapping.start(paramJob)

          //pricing sku data
          case DataSets.PRICING_SKU_DATA => SkuData.start(paramJob)

          // dcf feed
          case DataSets.DCF_FEED_GENERATE => DcfFeedGenerator.start(paramJob)

          // Ad4push files
          case DataSets.AD4PUSH_DEVICE_MERGER => Ad4pushDeviceMerger.start(paramJob, isHistory)
          case DataSets.AD4PUSH_CUSTOMER_RESPONSE => DevicesReactions.start(paramJob)

          // generate recommendations
          case DataSets.RECOMMENDATIONS => RecommendationGenerator.start(paramJob)

          // all pushCampaign quality checks
          case DataSets.CAMPAIGN_QUALITY => CampaignQualityEntry.start(paramJob)
          //email campaign feed files.
          case DataSets.CUST_WELCOME_VOUCHER => CustWelcomeVoucher.start(paramJob)
          case DataSets.CUST_PREFERENCE => CustPreference.start(paramJob)
          case DataSets.CUST_TOP5 => CustTop5.start(paramJob)
          case DataSets.CUSTOMER_ORDERS => CustomerOrders.start(paramJob)
          case DataSets.CONTACT_LIST_MOBILE => ContactListMobile.start(paramJob)
          case DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2 => CustomerPreferredTimeslotPart2.start(paramJob)
          case DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1 => CustomerPreferredTimeslotPart1.start(paramJob)
          case DataSets.PAYBACK_DATA => PaybackData.start(paramJob)
          case DataSets.SALES_ORDER_ADDR_FAV => SalesOrderAddress.start(paramJob)
          case DataSets.SALES_ITEM_REVENUE => SalesItemRevenue.start(paramJob)
          case DataSets.CUSTOMER_APP_DETAILS => CustomerAppDetails.start(paramJob)
          case DataSets.SHOP_THE_LOOK => ShoopTheLook.start(paramJob)
          //CUSTOMER_SURF_AFFINITY from surf data
          case DataSets.CUSTOMER_SURF_AFFINITY => CustomerSurfAffinity.start(paramJob)

          // responsys files
          case DataSets.DND_MERGER => DNDMerger.start(paramJob)
          case DataSets.SMS_OPT_OUT_MERGER => SmsOptOut.start(paramJob)
          case DataSets.CUST_EMAIL_RESPONSE => CustEmailResponse.start(paramJob)

          //// clickstream use cases
          case DataSets.CLICKSTREAM_YESTERDAY_SESSION => SurfVariablesMain.startClickstreamYesterdaySessionVariables(paramJob)
          case DataSets.CLICKSTREAM_SURF3_VARIABLE => SurfVariablesMain.startSurf3Variable(paramJob)
          case DataSets.CLICKSTREAM_SURF3_MERGED_DATA30 => GetSurfVariables.getSurf3mergedForLast30Days(paramJob)
          case DataSets.CLICKSTREAM_DATA_QUALITY => DataQualityMethods.start(paramJob)

          // campaigns
          case DataSets.ACART_HOURLY => CampaignManager.startAcartHourlyCampaign(paramJob)

          case CampaignCommon.FOLLOW_UP_CAMPAIGNS => CampaignManager.startFollowUpCampaigns(paramJob)

          //calendar campaigns
          case CampaignCommon.PRICEPOINT_CAMPAIGN => CampaignManager.startPricepointCampaign(paramJob)
          case CampaignCommon.HOTTEST_X_CAMPAIGN => CampaignManager.startHottestXCampaign(paramJob)
          case CampaignCommon.REPLENISHMENT_CAMPAIGN => CampaignManager.startReplenishmentCampaign(paramJob)
          case CampaignCommon.REPLENISHMENT_CAMPAIGN_FEED => CampaignManager.replenishmentFeed(paramJob)
          case CampaignCommon.BRAND_IN_CITY_CAMPAIGN => CampaignManager.startBrandInCityCampaign(paramJob)
          case CampaignCommon.BRICK_AFFINITY_CAMPAIGN => CampaignManager.startBrickAffinityCampaign(paramJob)
          case CampaignCommon.GEO_CAMPAIGN => CampaignManager.startGeoCampaigns(paramJob)
          case CampaignCommon.LOVE_CALENDAR_CAMPAIGNS => CampaignManager.startLoveCampaigns(paramJob)
          case CampaignCommon.CLEARANCE_CAMPAIGN => CampaignManager.startClearanceCampaign(paramJob)

          // miscellaneous data
          case DataSets.CITY_WISE_DATA => CityData.start(paramJob)
          case DataSets.WINBACK_CUSTOMER => WinbackData.start(paramJob)

          case DataSets.MONGO_FEED_GENERATOR => MongoFeedGenerator.start(paramJob)

          case DataSets.CUSTOMER_MASTER_RECORD_FEED => CustomerMasterRecordFeed.start(paramJob)
          case _ => logger.error("Unknown source.")

        }
      }
    }

  }

}
