package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.calendarcampaign._
import com.jabong.dap.campaign.campaignlist._
import com.jabong.dap.campaign.data.{ CampaignInput, CampaignOutput }
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields, Recommendation }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.{ CampaignConfig, CampaignInfo, ParamInfo }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

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

  def startRetargetCampaigns() = {
    val liveRetargetCampaign = new LiveRetargetCampaign()
    val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val orderItemData = CampaignInput.loadYesterdayOrderItemData().cache()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val orderData = CampaignInput.loadLastNDaysTableData(30, fullOrderData, SalesOrderVariables.CREATED_AT)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    liveRetargetCampaign.runCampaign(orderData, orderItemData, yesterdayItrData, brickMvpRecommendations, yestDate)
  }

  def startPricepointCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)
    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    val fullOrderData = CampaignInput.loadFullOrderData(incrDate)
    //val last20thDaySalesOrderData = CampaignInput.loadNthdayTableData(20, fullOrderData)
    val last20thDaySalesOrderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 20, 30)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)

    //val last20thDaySalesOrderItemData = CampaignInput.loadNthdayTableData(20, fullOrderItemData)
    val last20thDaySalesOrderItemData = CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 20, 30)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val brickPriceBandRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_PRICE_BAND_SUB_TYPE, incrDate).cache()

    val pricepointCampaign = new PricepointCampaign()
    pricepointCampaign.runCampaign(last20thDaySalesOrderData, last20thDaySalesOrderItemData, brickPriceBandRecommendations, yesterdayItrData, incrDate)

  }

  def startBrickAffinityCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullCustomerSurfAffinity = CampaignInput.loadFullVariablesData(DataSets.CUSTOMER_SURF_AFFINITY, incrDate)

    val fullOrderData = CampaignInput.loadFullOrderData(incrDate)

    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    //val last7thDaySalesOrderData = CampaignInput.loadNthdayTableData(7, fullOrderData)
    val last7thDaySalesOrderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 7, 30)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)

    //val last7thDaySalesOrderItemData = CampaignInput.loadNthdayTableData(7, fullOrderItemData)
    val last7thDaySalesOrderItemData = CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 7, 30)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE, incrDate).cache()

    val brickAffinityCampaign = new BrickAffinityCampaign()
    brickAffinityCampaign.runCampaign(fullCustomerSurfAffinity, last7thDaySalesOrderData, last7thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData, incrDate)

  }

  def startBrandInCityCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullCustomerOrders = CampaignInput.loadFullVariablesData(DataSets.CUSTOMER_ORDERS, incrDate)
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(SalesOrderItemVariables.FAV_BRAND) as ProductVariables.BRAND,
        col(ContactListMobileVars.CITY) as CustomerVariables.CITY
      //col(SalesOrderVariables.LAST_ORDER_DATE) as SalesOrderVariables.CREATED_AT
      ).distinct

    /*val last6thDaysCustomerOrderData = CampaignInput.loadLastNdaysOrderData(7, fullCustomerOrders, incrDate)
      .drop(SalesOrderVariables.CREATED_AT)
      .distinct*/

    val fullOrderData = CampaignInput.loadFullOrderData(incrDate)

    //    val last6thDaySalesOrderData = CampaignInput.loadNthdayTableData(6, fullOrderData)

    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)
    val last6thDaySalesOrderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 5, 30)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)

    // val last6thDaySalesOrderItemData = CampaignInput.loadNthdayTableData(6, fullOrderItemData)
    val last6thDaySalesOrderItemData = CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 5, 30)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val brandMvpCityRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_CITY_SUB_TYPE, incrDate).cache()

    val brandInCityCampaign = new BrandInCityCampaign()
    brandInCityCampaign.runCampaign(fullCustomerOrders, last6thDaySalesOrderData, last6thDaySalesOrderItemData, brandMvpCityRecommendations, yesterdayItrData, incrDate)

  }

  /**
   *
   * @param params
   */
  def startReplenishmentCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val customerOrderFull = CampaignInput.loadFullVariablesData(DataSets.CUSTOMER_ORDERS, incrDate).
      select(col(CustomerVariables.FK_CUSTOMER),
        col(SalesOrderItemVariables.SUCCESSFUL_ORDERS),
        col(SalesOrderVariables.LAST_ORDER_DATE) as SalesOrderVariables.CREATED_AT).distinct

    val fullSalesOrderData = CampaignInput.loadFullOrderData(incrDate)

    val fullSalesOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)

    val lastYearCustomerOrderFull = CampaignInput.loadLastNDaysTableData(370, customerOrderFull, SalesOrderVariables.CREATED_AT, incrDate)

    val lastYearSalesOrderData = CampaignInput.loadLastNDaysTableData(370, fullSalesOrderData, SalesOrderVariables.CREATED_AT).
      select(SalesOrderVariables.FK_CUSTOMER,
        SalesOrderVariables.CUSTOMER_EMAIL,
        SalesOrderVariables.ID_SALES_ORDER,
        SalesOrderVariables.CREATED_AT,
        SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING)

    val lastYearSalesOrderItemData = CampaignInput.loadLastNDaysTableData(370, fullSalesOrderItemData, SalesOrderVariables.UPDATED_AT).
      select(SalesOrderItemVariables.FK_SALES_ORDER,
        SalesOrderItemVariables.SKU,
        SalesOrderItemVariables.CREATED_AT,
        SalesOrderItemVariables.PAID_PRICE)

    CampaignUtils.debug(fullSalesOrderItemData, "fullSalesOrderItemData")

    CampaignUtils.debug(lastYearSalesOrderItemData, "lastYearSalesOrderItemData")

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE, incrDate).cache()

    val replenishmentCampaign = new ReplenishmentCampaign()
    replenishmentCampaign.runCampaign(lastYearCustomerOrderFull, lastYearSalesOrderData, lastYearSalesOrderItemData, brickMvpRecommendations, yesterdayItrData, incrDate)

  }

  def replenishmentFeed(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    //    val cmr = CampaignInput.loadCustomerMasterData()
    //      .select(
    //        CustomerVariables.EMAIL,
    //        ContactListMobileVars.UID
    //      )

    val dfReplenishment = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.REPLENISHMENT_CAMPAIGN_NO_CMR, DataSets.DAILY_MODE, incrDate)
    //    val dfReplenishmentJoinToCmr = dfReplenishment.join(cmr, dfReplenishment(CustomerVariables.EMAIL) === cmr(CustomerVariables.EMAIL), SQL.INNER)
    //      .select(
    //        dfReplenishment("*"),
    //        cmr(ContactListMobileVars.UID)
    //      )

    val dfReplenishmentJoinToCmr = CampaignUtils.getSelectedReplenishAttributes(dfReplenishment, incrDate)

    CampaignOutput.saveCampaignData(dfReplenishmentJoinToCmr, CampaignCommon.REPLENISHMENT_CAMPAIGN, DataSets.CALENDAR_CAMPAIGNS, incrDate)

  }

  /**
   * starting point of love campaigns
   * @param params
   */
  def startLoveCampaigns(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeUtils.YESTERDAY_FOLDER))
    val salesOrderFullData = CampaignInput.loadFullOrderData(incrDate)
    val salesOrderItemFullData = CampaignInput.loadFullOrderItemData(incrDate)

    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    //val last35thSalesOrderData = CampaignInput.loadNthdayTableData(35, salesOrderFullData)
    //val last35thSalesOrderItemData = CampaignInput.loadNthdayTableData(35, salesOrderItemFullData)
    val last35thSalesOrderData = CampaignInput.loadNthDayModData(salesOrderFullData, incrDate1, 35, 60)
    val last35thSalesOrderItemData = CampaignInput.loadNthDayModData(salesOrderItemFullData, incrDate1, 35, 60)

    val customerTopData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.MAPS, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, incrDate)
    //val last15thSalesOrderData = CampaignInput.loadNthdayTableData(15, salesOrderFullData)
    //val last15thSalesOrderItemData = CampaignInput.loadNthdayTableData(15, salesOrderItemFullData)
    val last15thSalesOrderData = CampaignInput.loadNthDayModData(salesOrderFullData, incrDate1, 15, 30)
    val last15thSalesOrderItemData = CampaignInput.loadNthDayModData(salesOrderItemFullData, incrDate1, 15, 30)

    val yesterdayItrSkuSimple = CampaignInput.loadYesterdayItrSimpleData(incrDate)

    val brandMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_SUB_TYPE, incrDate).cache()

    val mvpColorRecommendations = CampaignInput.loadRecommendationData(Recommendation.MVP_COLOR_SUB_TYPE, incrDate).cache()

    val loveBrandCampaign = new LoveBrandCampaign()

    loveBrandCampaign.runCampaign(customerTopData, last35thSalesOrderData, last35thSalesOrderItemData, brandMvpRecommendations, yesterdayItrSkuSimple, incrDate)

    val loveColorCampaign = new LoveColorCampaign()

    loveColorCampaign.runCampaign(customerTopData, last15thSalesOrderData, last15thSalesOrderItemData, mvpColorRecommendations, yesterdayItrSkuSimple, incrDate)

  }

  def startInvalidCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    // invalid followup
    val fullOrderData = CampaignInput.loadFullOrderData()

    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData(DataSets.PUSH_CAMPAIGNS)
    val orderData = CampaignInput.loadLastNDaysTableData(30, fullOrderData, SalesOrderVariables.CREATED_AT)

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    // last 3 days of orderitem data
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val orderItemData = CampaignInput.loadLastNDaysTableData(3, fullOrderItemData, SalesOrderVariables.UPDATED_AT)

    // yesterday itr - Qty of Ref SKU to be greater than/equal to 10
    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData()

    val last30DaysItrData = CampaignInput.load30DayItrSkuSimpleData()
    val invalidFollowUp = new InvalidFollowUpCampaign()
    invalidFollowUp.runCampaign(orderData, orderItemData, yesterdayItrData, brickMvpRecommendations, yestDate)

    // invalid lowstock
    // last 30 days of order item data
    val last30DayOrderItemData = CampaignInput.loadLastNDaysTableData(30, fullOrderItemData, SalesOrderVariables.UPDATED_AT)

    // last 2 months order data
    val last60DayOrderData = CampaignInput.loadLastNDaysTableData(60, fullOrderData, SalesOrderVariables.CREATED_AT)

    val invalidLowStock = new InvalidLowStockCampaign()
    invalidLowStock.runCampaign(last60DayOrderData, last30DayOrderItemData, yesterdayItrData, brickMvpRecommendations, yestDate)

    // invalid iod campaign
    val invalidIODCampaign = new InvalidIODCampaign()
    invalidIODCampaign.runCampaign(orderData, orderItemData, last30DaysItrData, brickMvpRecommendations, yestDate)
  }

  /**
   *
   * @param campaignsConfig
   */
  def startAbandonedCartCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    // acart daily, acart followup, acart low stock, acart iod
    val last30DayAcartData = CampaignInput.loadLast30daysAcartData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()
    // val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()

    // load common recommendations
    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()
    // acart daily - last day acart data, ref sku not bought on last day
    // no previous campaign check
    // FIXME: search for email
    val yesterdayAcartData = CampaignInput.loadNthDayTableData(1, last30DayAcartData, SalesOrderVariables.CREATED_AT)
    val yesterdaySalesOrderItemData = CampaignInput.loadYesterdayOrderItemData() // created_at
    val yesterdaySalesOrderData = CampaignInput.loadLastNDaysTableData(1, fullOrderData, SalesOrderVariables.CREATED_AT)
    val acartDaily = new AcartDailyCampaign()
    acartDaily.runCampaign(yesterdayAcartData, yesterdaySalesOrderData, yesterdaySalesOrderItemData, yesterdayItrData, brickMvpRecommendations, yestDate)

    // acart followup - only = 3rd days acart, still not bought ref skus, qty >= 10, yesterdayItrData
    val prev3rdDayAcartData = CampaignInput.loadNthDayTableData(3, last30DayAcartData, SalesOrderVariables.CREATED_AT)
    val last3DaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(3, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at
    val last3DaySalesOrderData = CampaignInput.loadLastNDaysTableData(3, fullOrderData, SalesOrderVariables.CREATED_AT)

    val acartFollowup = new AcartFollowUpCampaign()
    acartFollowup.runCampaign(prev3rdDayAcartData, last3DaySalesOrderData, last3DaySalesOrderItemData, yesterdayItrData, brickMvpRecommendations, yestDate)

    // FIXME: part of customerselction for iod and lowstock can be merged

    // low stock - last 30 day acart (last30DayAcartData), yesterdayItrData, qty <=10
    //  yesterdayItrData
    // have not placed the order
    val last30DaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(30, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNDaysTableData(30, fullOrderData, SalesOrderVariables.CREATED_AT)
    val acartLowStock = new AcartLowStockCampaign()
    acartLowStock.runCampaign(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, yesterdayItrData, brickMvpRecommendations, yestDate)

    // item on discount
    // last30DayAcartData
    // last30DaySalesOrderItemData = null  // created_at
    // last30DaySalesOrderData = null

    // itr last 30 days
    val last30daysItrData = CampaignInput.load30DayItrSkuSimpleData()

    val acartIOD = new AcartIODCampaign() //FIXME: RUN ACart Campaigns
    acartIOD.runCampaign(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, last30daysItrData, brickMvpRecommendations, yestDate)

    //Start: Shortlist Reminder email Campaign
    val recommendationsData = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE)

  }

  /**
   *
   * @param params
   */
  def startAcartHourlyCampaign(params: ParamInfo) = {
    val incrDateWithHour = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterHours(0, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER))
    val lastHour = -2
    val salesCartHourly = CampaignInput.loadNthHourTableData(DataSets.SALES_CART, lastHour, incrDateWithHour)
    val salesOrderHourly = CampaignInput.loadNHoursTableData(DataSets.SALES_ORDER, lastHour, incrDateWithHour)
    val salesOrderItemHourly = CampaignInput.loadNthHourTableData(DataSets.SALES_ORDER_ITEM, lastHour, incrDateWithHour)
    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData()
    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    val acartHourly = new AcartHourlyCampaign()

    acartHourly.runCampaign(salesCartHourly, salesOrderHourly, salesOrderItemHourly, yesterdayItrData, brickMvpRecommendations, incrDateWithHour)

  }
  //  val campaignPriority = udf((mailType: Int) => CampaignUtils.getCampaignPriority(mailType: Int, mailTypePriorityMap: scala.collection.mutable.HashMap[Int, Int]))

  def startWishlistCampaigns(campaignsConfig: String) = {

    CampaignManager.initCampaignsConfig(campaignsConfig)

    val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val fullShortlistData = CampaignInput.loadFullShortlistData()

    val last30DaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(30, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNDaysTableData(30, fullOrderData, SalesOrderVariables.CREATED_AT)

    val yesterdaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(1, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at
    val yesterdaySalesOrderData = CampaignInput.loadLastNDaysTableData(1, fullOrderData, SalesOrderVariables.CREATED_AT)

    //    val todayDate = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS)

    val shortlistYesterdayData = CampaignInput.loadNthDayTableData(1, fullShortlistData, SalesOrderVariables.CREATED_AT)

    val shortlistLast30DayData = CampaignInput.loadLastNDaysTableData(30, fullShortlistData, CustomerVariables.CREATED_AT)
    val itrSkuYesterdayData = CampaignInput.loadYesterdayItrSkuData()
    val itrSkuSimpleYesterdayData = CampaignInput.loadYesterdayItrSimpleData()

    //    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()

    // call iod campaign
    val itrSku30DayData = CampaignInput.load30DayItrSkuData()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    val wishListCampaign = new WishListCampaign()
    wishListCampaign.runCampaign(shortlistYesterdayData,
      shortlistLast30DayData,
      itrSkuYesterdayData,
      itrSkuSimpleYesterdayData,
      yesterdaySalesOrderData,
      yesterdaySalesOrderItemData,
      last30DaySalesOrderData,
      last30DaySalesOrderItemData,
      itrSku30DayData,
      brickMvpRecommendations,
      yestDate)

    //Start: Shortlist Reminder email Campaign
    val recommendationsData = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE)
    val shortlist3rdDayData = CampaignInput.loadNthDayTableData(3, fullShortlistData, SalesOrderVariables.CREATED_AT)

    val shortlistReminderCampaign = new ShortlistReminderCampaign()
    shortlistReminderCampaign.runCampaign(shortlist3rdDayData, recommendationsData, itrSkuSimpleYesterdayData, yestDate)

  }

  def startSurfCampaigns(campaignsConfig: String) = {

    CampaignManager.initCampaignsConfig(campaignsConfig)
    val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val yestSurfSessionData = CampaignInput.loadYesterdaySurfSessionData().cache()
    val yestItrSkuData = CampaignInput.loadYesterdayItrSkuData().cache()
    val customerMasterData = CampaignInput.loadCustomerMasterData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val yestOrderData = CampaignInput.loadLastNDaysTableData(1, fullOrderData, SalesOrderVariables.CREATED_AT)
    val yestOrderItemData = CampaignInput.loadYesterdayOrderItemData()

    //surf3
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last30DaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(30, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNDaysTableData(30, fullOrderData, SalesOrderVariables.CREATED_AT)
    val lastDaySurf3Data = CampaignInput.loadLastDaySurf3Data()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    val surfCampaign = new SurfCampaign()

    surfCampaign.runCampaign(
      yestSurfSessionData,
      yestItrSkuData,
      customerMasterData,
      yestOrderData,
      yestOrderItemData,
      lastDaySurf3Data,
      last30DaySalesOrderData,
      last30DaySalesOrderItemData,
      brickMvpRecommendations,
      yestDate
    )

  }

  def startMiscellaneousCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    val yestDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    //loading brickmvp recommendations
    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()
    //loading brandmvp recommendations
    val brandMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_SUB_TYPE).cache()

    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last30DaySalesOrderData = CampaignInput.loadLastNDaysTableData(30, fullOrderData, SalesOrderVariables.CREATED_AT)
    val yesterdaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(1, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at
    val itrSkuSimpleYesterdayData = CampaignInput.loadYesterdayItrSimpleData()

    //Start: MIPR email Campaign
    val miprCampaign = new MIPRCampaign()
    miprCampaign.runCampaign(last30DaySalesOrderData, yesterdaySalesOrderItemData, brickMvpRecommendations, itrSkuSimpleYesterdayData, yestDate)
    val last30DayAcartData = CampaignInput.loadLast30daysAcartData()

    val last30DaySalesOrderItemData = CampaignInput.loadLastNDaysTableData(30, fullOrderItemData, SalesOrderVariables.UPDATED_AT) // created_at

    //Start: New Arrival email Campaign
    val newArrivalsBrandCampaign = new NewArrivalsBrandCampaign()
    newArrivalsBrandCampaign.runCampaign(last30DaySalesOrderData, last30DaySalesOrderItemData, last30DayAcartData, brandMvpRecommendations, itrSkuSimpleYesterdayData, yestDate)
  }

  def startHottestXCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)
    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    val genderMvpBrickRecos = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE, incrDate)

    val fullOrderData = CampaignInput.loadFullOrderData(incrDate)
    val fullOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)

    //val last60DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(60, fullOrderData)
    //val last60DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderData(60, fullOrderItemData)

    val itrYesterdayData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val hottestXCampaign = new HottestXCampaign()

    hottestXCampaign.runCampaign(fullOrderData, fullOrderItemData, itrYesterdayData, genderMvpBrickRecos, incrDate)

  }

  /**
   *  save acart hourly campaign Feed
   * @param campaignName
   */
  def acartHourlyFeed(campaignName: String) = {

    val cmr = CampaignInput.loadCustomerMasterData()
    //    if(campaignName.equals(DataSets.ACART_HOURLY)){
    val acartHourly = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EMAIL_CAMPAIGNS, CampaignCommon.ACART_HOURLY_CAMPAIGN, DataSets.HOURLY_MODE, TimeUtils.CURRENT_HOUR_FOLDER)

    val acartHourlyFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_" + TimeUtils.getHour(TimeUtils.getTodayDate(TimeConstants.DD_MMM_YYYY_HH_MM_SS), TimeConstants.DD_MMM_YYYY_HH_MM_SS) + "_LIVE_ACART_HOURLY"

    CampaignOutput.saveAcartHourlyFeed(acartHourly, cmr, acartHourlyFileName)
  }

  def startFollowUpCampaigns(params: ParamInfo) = {
    val fullOrderData = CampaignInput.loadFullOrderData()
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))

    //  val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last3DaySalesOrderData = CampaignInput.loadLastNDaysTableData(3, fullOrderData, SalesOrderVariables.CREATED_AT, incrDate)
    //    val yesterdaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(1, fullOrderItemData) // created_at
    val itrSkYesterdayData = CampaignInput.loadYesterdayItrSkuData()

    val ThirdDayCampaignMergedData = CampaignInput.loadNthDayCampaignMergedData(DataSets.EMAIL_CAMPAIGNS, 3, incrDate)
    //Start: FollowUp email Campaign
    val followUpCampaigns = new FollowUpCampaigns()
    followUpCampaigns.runCampaign(ThirdDayCampaignMergedData, last3DaySalesOrderData, itrSkYesterdayData, incrDate)
  }

  def startGeoCampaigns(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    val fullOrderData = CampaignInput.loadFullOrderData(incrDate)
    val day40_orderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 40, 60)
    val day50_orderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 50, 60)
    //val day40_orderData = CampaignInput.loadNthdayTableData(40, fullOrderData)
    //val day50_orderData = CampaignInput.loadNthdayTableData(50, fullOrderData)

    val genderMvpBrickRecos = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE, incrDate).cache()
    val genderMvpBrandRecos = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_SUB_TYPE, incrDate).cache()

    val fullOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)
    val day40_orderItemData = CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 40, 60)
    val day50_orderItemData = CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 50, 60)
    //val day40_orderItemData = CampaignInput.loadNthdayTableData(40, fullOrderItemData)
    //val day50_orderItemData = CampaignInput.loadNthdayTableData(50, fullOrderItemData)

    val salesAddressData = CampaignInput.loadSalesAddressData(incrDate)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val cityWiseData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CITY_WISE_DATA, DataSets.FULL_MERGE_MODE, incrDate)

    val geoStyleCampaign = new GeoStyleCampaign
    geoStyleCampaign.runCampaign(day40_orderData, day40_orderItemData, salesAddressData, yesterdayItrData, cityWiseData, genderMvpBrickRecos, incrDate)

    val geoBrandCampaign = new GeoBrandCampaign
    geoBrandCampaign.runCampaign(day50_orderData, day50_orderItemData, salesAddressData, yesterdayItrData, cityWiseData, genderMvpBrandRecos, incrDate)

  }

  def startClearanceCampaign(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullOrderData = CampaignInput.loadFullOrderData(incrDate)
    //val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData, incrDate)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData(incrDate)
    //val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData, incrDate)

    val mvpDiscountRecos = CampaignInput.loadRecommendationData(Recommendation.MVP_DISCOUNT_SUB_TYPE, incrDate).cache()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData(incrDate).cache()

    val clearanceCampaign = new ClearanceCampaign
    clearanceCampaign.runCampaign(fullOrderData, fullOrderItemData, mvpDiscountRecos, yesterdayItrData, incrDate)
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
  def startCampaignMerge(campaignJsonPath: String, campaignType: String) = {
    require(Array(DataSets.EMAIL_CAMPAIGNS, DataSets.PUSH_CAMPAIGNS, DataSets.CALENDAR_CAMPAIGNS) contains campaignType)

    if (CampaignManager.initCampaignsConfigJson(campaignJsonPath)) {
      //      createCampaignMaps(json)
      val saveMode = DataSets.OVERWRITE_SAVEMODE
      val dateFolder = TimeUtils.YESTERDAY_FOLDER
      val allCampaignsData = CampaignInput.loadAllCampaignsData(dateFolder, campaignType)
      val cmr = CampaignInput.loadCustomerMasterData(dateFolder)

      val mergedData =
        if (DataSets.PUSH_CAMPAIGNS == campaignType) {
          val allCamp = CampaignProcessor.mapDeviceFromCMR(cmr, allCampaignsData)
          val itr = CampaignInput.loadYesterdayItrSkuDataForCampaignMerge()
          CampaignProcessor.mergePushCampaigns(allCamp, itr).coalesce(1).cache()
        } else {
          val allCamp = CampaignProcessor.mapEmailCampaignWithCMR(cmr, allCampaignsData)
          CampaignProcessor.mergeEmailCampaign(allCamp)
        }

      CampaignUtils.debug(mergedData, "merged data frame for" + campaignType)
      println("Starting write parquet after repartitioning and caching for " + campaignType)
      val writePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, CampaignCommon.MERGED_CAMPAIGN, DataSets.DAILY_MODE, dateFolder)
      if (campaignType == DataSets.PUSH_CAMPAIGNS) {
        val ad4push = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.AD4PUSH_ID, DataSets.FULL_MERGE_MODE, dateFolder)
        val finalCampaign = CampaignProcessor.addAd4pushId(ad4push, mergedData)
        val iosDF = finalCampaign.filter(finalCampaign(CampaignMergedFields.DOMAIN) === DataSets.IOS)
        val androidDF = finalCampaign.filter(finalCampaign(CampaignMergedFields.DOMAIN) === DataSets.ANDROID).na.drop(Array(PageVisitVariables.ADD4PUSH))

        val mergedAd4push = iosDF.unionAll(androidDF)
        println("Starting write parquet after repartitioning and caching")
        val writePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, campaignType, CampaignCommon.MERGED_CAMPAIGN, DataSets.DAILY_MODE, dateFolder)
        if (DataWriter.canWrite(saveMode, writePath))
          DataWriter.writeParquet(mergedAd4push, writePath, saveMode)

        //writing csv file
        CampaignProcessor.splitFileToCSV(iosDF, androidDF, dateFolder)
      } else if (campaignType == DataSets.EMAIL_CAMPAIGNS) {
        val GARBAGE = "NA" //:TODO replace with correct value
        val temp = "temp"
        val expectedDF = mergedData.withColumnRenamed(CampaignMergedFields.LIVE_CART_URL, CampaignMergedFields.LIVE_CART_URL + temp)
          .withColumn(ContactListMobileVars.UID, col(ContactListMobileVars.UID))
          .withColumn(ContactListMobileVars.EMAIL, Udf.maskForDecrypt(col(CampaignMergedFields.EMAIL), lit("**")))
          .withColumn(CampaignMergedFields.LIVE_MAIL_TYPE, col(CampaignMergedFields.CAMPAIGN_MAIL_TYPE))
          .withColumn(CampaignMergedFields.LIVE_BRAND, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(1)))
          .withColumn(CampaignMergedFields.LIVE_BRICK, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(2)))
          .withColumn(CampaignMergedFields.LIVE_PROD_NAME, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(3)))

          .withColumn(CampaignMergedFields.LIVE_REF_SKU + "1", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(0)))
          .withColumn(CampaignMergedFields.LIVE_REF_SKU + "2", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(1), lit(0)))
          .withColumn(CampaignMergedFields.LIVE_REF_SKU + "3", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(2), lit(0)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "1", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(0)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "2", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(1)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "3", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(2)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "4", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(3)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "5", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(4)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "6", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(5)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "7", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(6)))
          .withColumn(CampaignMergedFields.LIVE_REC_SKU + "8", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(7)))

          .withColumn(CampaignMergedFields.LIVE_CART_URL, when(col(CampaignMergedFields.LIVE_CART_URL + temp).isNull, lit("")).otherwise(col(CampaignMergedFields.LIVE_CART_URL + temp)))
          .withColumn(CampaignMergedFields.LAST_UPDATED_DATE, lit(TimeUtils.yesterday(TimeConstants.DATE_FORMAT)))
          .withColumn(ContactListMobileVars.MOBILE, lit(GARBAGE))
          .withColumn(CampaignMergedFields.TYPO_MOBILE_PERMISION_STATUS, lit(GARBAGE))
          .withColumn(CampaignMergedFields.COUNTRY_CODE, lit(GARBAGE))
          .drop(CustomerVariables.EMAIL)
          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
          .drop(CampaignMergedFields.LIVE_CART_URL + temp)

        //        val emailCampaignFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_LIVE_CAMPAIGN"
        //        val csvDataFrame = expectedDF.drop(CampaignMergedFields.CUSTOMER_ID)
        // .drop(CampaignMergedFields.REF_SKUS)
        // .drop(CampaignMergedFields.REC_SKUS)
        CampaignUtils.debug(expectedDF, "expectedDF final before writing data frame for" + campaignType)
        DataWriter.writeParquet(expectedDF, writePath, saveMode)
        //        DataWriter.writeCsv(csvDataFrame, DataSets.CAMPAIGNS, DataSets.EMAIL_CAMPAIGNS, DataSets.DAILY_MODE, dateFolder, emailCampaignFileName, saveMode, "true", ";")
      } else if (campaignType == DataSets.CALENDAR_CAMPAIGNS) {
        val GARBAGE = "NA" //:TODO replace with correct value
        val temp = "temp"
        val expectedDF = mergedData
          .withColumn(ContactListMobileVars.UID, col(ContactListMobileVars.UID))
          .withColumn(CampaignMergedFields.CALENDAR_REF_BRAND, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(1)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_BRAND + "1", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(1)))
          .withColumn(CampaignMergedFields.CALENDAR_REF_BRICK, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(2)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_BRICK + "1", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(2)))

          .withColumn(CampaignMergedFields.CALENDAR_CITY, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(5)))
          .withColumn(CampaignMergedFields.CALENDAR_COLOR, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(4)))
          .withColumn(CampaignMergedFields.CALENDAR_PRICE_POINT, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(7)))
          .withColumn(CampaignMergedFields.CALENDAR_MAIL_TYPE, col(CampaignMergedFields.CAMPAIGN_MAIL_TYPE))

          //          .withColumn(ContactListMobileVars.EMAIL, Udf.addString(col(CampaignMergedFields.EMAIL), lit("**")))

          .withColumn(CampaignMergedFields.CALENDAR_REF_SKU + "1", Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(0)))
          .withColumn(CampaignMergedFields.CALENDAR_REF_SKU + "2", lit(""))
          .withColumn(CampaignMergedFields.CALENDAR_REF_SKU + "3", lit(""))
          .withColumn(CampaignMergedFields.CALENDAR_REF_SKU + "4", lit(""))

          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "1", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(0)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "2", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(1)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "3", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(2)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "4", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(3)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "5", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(4)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "6", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(5)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "7", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(6)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "8", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(7)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "9", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(8)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "10", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(9)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "11", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(10)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "12", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(11)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "13", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(12)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "14", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(13)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "15", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(14)))
          .withColumn(CampaignMergedFields.CALENDAR_REC_SKU + "16", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(15)))
          .withColumn(CampaignMergedFields.LAST_UPDATED_DATE, lit(TimeUtils.yesterday(TimeConstants.DATE_FORMAT)))
          .drop(CustomerVariables.EMAIL)
          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
          .drop(CampaignMergedFields.LIVE_CART_URL)

        //        val calendarCampaignFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_DCF_CAMPAIGN"
        //        val csvDataFrame = expectedDF.drop(CampaignMergedFields.CUSTOMER_ID)
        // .drop(CampaignMergedFields.REF_SKUS)
        // .drop(CampaignMergedFields.REC_SKUS)
        CampaignUtils.debug(expectedDF, "expectedDF final before writing data frame for" + campaignType)
        DataWriter.writeParquet(expectedDF, writePath, saveMode)
        //        DataWriter.writeCsv(csvDataFrame, DataSets.CAMPAIGNS, DataSets.CALENDAR_CAMPAIGNS, DataSets.DAILY_MODE, dateFolder, calendarCampaignFileName, saveMode, "true", ";")
      }
    }
  }

  def campaignMergeFeed(campaignType: String) = {

    val saveMode = DataSets.OVERWRITE_SAVEMODE
    val dateFolder = TimeUtils.YESTERDAY_FOLDER

    val (campaignFileName, csvDataFrame) = {

      val df = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, campaignType, CampaignCommon.MERGED_CAMPAIGN, DataSets.DAILY_MODE, dateFolder)

      if (campaignType == DataSets.EMAIL_CAMPAIGNS) {
        val csvDf =
          df.select(
            df(ContactListMobileVars.UID),
            df(ContactListMobileVars.EMAIL),
            df(CampaignMergedFields.LIVE_MAIL_TYPE).cast(StringType) as CampaignMergedFields.LIVE_MAIL_TYPE,
            df(CampaignMergedFields.LIVE_BRAND),
            df(CampaignMergedFields.LIVE_BRICK),
            df(CampaignMergedFields.LIVE_PROD_NAME),
            df(CampaignMergedFields.LIVE_REF_SKU + "1"),
            df(CampaignMergedFields.LIVE_REF_SKU + "2"),
            df(CampaignMergedFields.LIVE_REF_SKU + "3"),
            df(CampaignMergedFields.LIVE_REC_SKU + "1"),
            df(CampaignMergedFields.LIVE_REC_SKU + "2"),
            df(CampaignMergedFields.LIVE_REC_SKU + "3"),
            df(CampaignMergedFields.LIVE_REC_SKU + "4"),
            df(CampaignMergedFields.LIVE_REC_SKU + "5"),
            df(CampaignMergedFields.LIVE_REC_SKU + "6"),
            df(CampaignMergedFields.LIVE_REC_SKU + "7"),
            df(CampaignMergedFields.LIVE_REC_SKU + "8"),
            df(CampaignMergedFields.LIVE_CART_URL),
            df(CampaignMergedFields.LAST_UPDATED_DATE),
            df(ContactListMobileVars.MOBILE),
            df(CampaignMergedFields.TYPO_MOBILE_PERMISION_STATUS),
            df(CampaignMergedFields.COUNTRY_CODE)
          ).na.fill("")

        (TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_LIVE_CAMPAIGN", csvDf)
      } else {
        val csvDf = df.select(
          df(ContactListMobileVars.UID),
          df(CampaignMergedFields.CALENDAR_REF_BRAND),
          df(CampaignMergedFields.CALENDAR_REC_BRAND + "1"),
          df(CampaignMergedFields.CALENDAR_REF_BRICK),
          df(CampaignMergedFields.CALENDAR_REC_BRICK + "1"),
          df(CampaignMergedFields.CALENDAR_CITY),
          df(CampaignMergedFields.CALENDAR_COLOR),
          df(CampaignMergedFields.CALENDAR_PRICE_POINT),
          df(CampaignMergedFields.CALENDAR_MAIL_TYPE).cast(StringType) as CampaignMergedFields.CALENDAR_MAIL_TYPE,
          df(CampaignMergedFields.CALENDAR_REF_SKU + "1"),
          df(CampaignMergedFields.CALENDAR_REF_SKU + "2"),
          df(CampaignMergedFields.CALENDAR_REF_SKU + "3"),
          df(CampaignMergedFields.CALENDAR_REF_SKU + "4"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "1"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "2"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "3"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "4"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "5"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "6"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "7"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "8"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "9"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "10"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "11"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "12"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "13"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "14"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "15"),
          df(CampaignMergedFields.CALENDAR_REC_SKU + "16"),
          df(CampaignMergedFields.LAST_UPDATED_DATE)
        ).na.fill("")

        (TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_DCF_CAMPAIGN", csvDf)
      }
    }

    DataWriter.writeCsv(csvDataFrame, campaignType, CampaignCommon.MERGED_CAMPAIGN, DataSets.DAILY_MODE, dateFolder, campaignFileName, saveMode, "true", ";", 1)
  }
}
