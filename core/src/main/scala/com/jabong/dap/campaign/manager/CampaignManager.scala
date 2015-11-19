package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.calendarcampaign._
import com.jabong.dap.campaign.campaignlist._
import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CampaignMergedFields, Recommendation}
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ContactListMobileVars, CustomerVariables, PageVisitVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.{CampaignConfig, CampaignInfo, ParamInfo}
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

    val orderItemData = CampaignInput.loadYesterdayOrderItemData().cache()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val orderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    liveRetargetCampaign.runCampaign(orderData, orderItemData, yesterdayItrData, brickMvpRecommendations)
  }

  def startPricepointCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullOrderData = CampaignInput.loadFullOrderData()
    val last20thDaySalesOrderData = CampaignInput.loadLastNdaysOrderData(20, fullOrderData, incrDate)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    //FIXME
    val last20thDaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(20, fullOrderItemData, incrDate)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val brickPriceBandRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_PRICE_BAND_SUB_TYPE).cache()

    val pricepointCampaign = new PricepointCampaign()
    pricepointCampaign.runCampaign(last20thDaySalesOrderData, last20thDaySalesOrderItemData, brickPriceBandRecommendations, yesterdayItrData, incrDate)

  }

  def startBrickAffinityCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullCustomerSurfAffinity = CampaignInput.loadFullVariablesData(DataSets.CUSTOMER_SURF_AFFINITY)

    val fullOrderData = CampaignInput.loadFullOrderData()
    //FIXME
    val last7thDaySalesOrderData = CampaignInput.loadLastNdaysOrderData(7, fullOrderData, incrDate)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    //FIXME
    val last7thDaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(7, fullOrderItemData, incrDate)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    val brickAffinityCampaign = new BrickAffinityCampaign()
    brickAffinityCampaign.runCampaign(fullCustomerSurfAffinity, last7thDaySalesOrderData, last7thDaySalesOrderItemData, brickMvpRecommendations, yesterdayItrData, incrDate)

  }

  def startBrandInCityCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullCustomerOrders = CampaignInput.loadFullVariablesData(DataSets.CUSTOMER_ORDERS)

    val fullOrderData = CampaignInput.loadFullOrderData()
    //FIXME: loadLastNdaysOrderData
    val last6thDaySalesOrderData = CampaignInput.loadLastNdaysOrderData(6, fullOrderData, incrDate)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    //FIXME: loadLastNdaysOrderData
    val last6thDaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(6, fullOrderItemData, incrDate)

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val brandMvpCityRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_CITY_SUB_TYPE).cache()

    val brandInCityCampaign = new BrandInCityCampaign()
    brandInCityCampaign.runCampaign(fullCustomerOrders, last6thDaySalesOrderData, last6thDaySalesOrderItemData, brandMvpCityRecommendations, yesterdayItrData, incrDate)

  }

  def startReplenishmentCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val contactListMobileFull = CampaignInput.loadFullVariablesData(DataSets.CONTACT_LIST_MOBILE, incrDate)

    val fullSalesOrderData = CampaignInput.loadFullOrderData()

    val fullSalesOrderItemData = CampaignInput.loadFullOrderItemData()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    val replenishmentCampaign = new ReplenishmentCampaign()
    replenishmentCampaign.runCampaign(contactListMobileFull, fullSalesOrderData, fullSalesOrderItemData, brickMvpRecommendations, yesterdayItrData, incrDate)

  }

  /**
   * starting point of love campaigns
   * @param params
   */
  def startLoveCampaigns(params: ParamInfo): Unit = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeUtils.YESTERDAY_FOLDER))
    val salesOrderFullData = CampaignInput.loadFullOrderData(incrDate)
    val salesOrderItemFullData = CampaignInput.loadFullOrderItemData(incrDate)

    val last35thSalesOrderData = CampaignInput.loadNthdayTableData(35, salesOrderFullData)
    val last35thSalesOrderItemData = CampaignInput.loadNthdayTableData(35, salesOrderItemFullData)

    val customerTopData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.MAPS, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, incrDate)
    val last15thSalesOrderData = CampaignInput.loadNthdayTableData(15, salesOrderFullData)
    val last15thSalesOrderItemData = CampaignInput.loadNthdayTableData(15, salesOrderItemFullData)

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

    // invalid followup
    val fullOrderData = CampaignInput.loadFullOrderData()

    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()
    val orderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()

    // last 3 days of orderitem data
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val orderItemData = CampaignInput.loadLastNdaysOrderItemData(3, fullOrderItemData)

    // yesterday itr - Qty of Ref SKU to be greater than/equal to 10
    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData()

    val last30DaysItrData = CampaignInput.load30DayItrSkuSimpleData()
    val invalidFollowUp = new InvalidFollowUpCampaign()
    invalidFollowUp.runCampaign(orderData, orderItemData, yesterdayItrData, brickMvpRecommendations)

    // invalid lowstock
    // last 30 days of order item data
    val last30DayOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData)

    // last 2 months order data
    val last60DayOrderData = CampaignInput.loadLastNdaysOrderData(60, fullOrderData)

    val invalidLowStock = new InvalidLowStockCampaign()
    invalidLowStock.runCampaign(last60DayOrderData, last30DayOrderItemData, yesterdayItrData, brickMvpRecommendations)

    // invalid iod campaign
    val invalidIODCampaign = new InvalidIODCampaign()
    invalidIODCampaign.runCampaign(orderData, orderItemData, last30DaysItrData, brickMvpRecommendations)
  }

  /**
   *
   * @param campaignsConfig
   */
  def startAbandonedCartCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)

    // acart daily, acart followup, acart low stock, acart iod
    val last30DayAcartData = CampaignInput.loadLast30daysAcartData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData()
    // val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()

    // load common recommendations
    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()
    // acart daily - last day acart data, ref sku not bought on last day
    // no previous campaign check
    // FIXME: search for email
    val yesterdayAcartData = CampaignInput.loadNthdayAcartData(1, last30DayAcartData)
    val yesterdaySalesOrderItemData = CampaignInput.loadYesterdayOrderItemData() // created_at
    val yesterdaySalesOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)
    val acartDaily = new AcartDailyCampaign()
    acartDaily.runCampaign(yesterdayAcartData, yesterdaySalesOrderData, yesterdaySalesOrderItemData, yesterdayItrData, brickMvpRecommendations)

    // acart followup - only = 3rd days acart, still not bought ref skus, qty >= 10, yesterdayItrData
    val prev3rdDayAcartData = CampaignInput.loadNthdayAcartData(3, last30DayAcartData)
    val last3DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(3, fullOrderItemData) // created_at
    val last3DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(3, fullOrderData)

    val acartFollowup = new AcartFollowUpCampaign()
    acartFollowup.runCampaign(prev3rdDayAcartData, last3DaySalesOrderData, last3DaySalesOrderItemData, yesterdayItrData, brickMvpRecommendations)

    // FIXME: part of customerselction for iod and lowstock can be merged

    // low stock - last 30 day acart (last30DayAcartData), yesterdayItrData, qty <=10
    //  yesterdayItrData
    // have not placed the order
    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)
    val acartLowStock = new AcartLowStockCampaign()
    acartLowStock.runCampaign(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, yesterdayItrData, brickMvpRecommendations)

    // item on discount
    // last30DayAcartData
    // last30DaySalesOrderItemData = null  // created_at
    // last30DaySalesOrderData = null

    // itr last 30 days
    val last30daysItrData = CampaignInput.load30DayItrSkuSimpleData()

    val acartIOD = new AcartIODCampaign() //FIXME: RUN ACart Campaigns
    acartIOD.runCampaign(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData, last30daysItrData, brickMvpRecommendations)

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

    acartHourly.runCampaign(salesCartHourly, salesOrderHourly, salesOrderItemHourly, yesterdayItrData, brickMvpRecommendations)

  }
  //  val campaignPriority = udf((mailType: Int) => CampaignUtils.getCampaignPriority(mailType: Int, mailTypePriorityMap: scala.collection.mutable.HashMap[Int, Int]))

  def startWishlistCampaigns(campaignsConfig: String) = {

    CampaignManager.initCampaignsConfig(campaignsConfig)

    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val fullShortlistData = CampaignInput.loadFullShortlistData()

    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    val yesterdaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(1, fullOrderItemData) // created_at
    val yesterdaySalesOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)

    val todayDate = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS)

    val shortlistYesterdayData = CampaignInput.loadNthDayShortlistData(fullShortlistData, 1, todayDate)

    val shortlistLast30DayData = CampaignInput.loadNDaysShortlistData(fullShortlistData, 30, todayDate)
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
      brickMvpRecommendations)

    //Start: Shortlist Reminder email Campaign
    val recommendationsData = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE)
    val shortlist3rdDayData = CampaignInput.loadNthDayShortlistData(fullShortlistData, 3, todayDate)

    val shortlistReminderCampaign = new ShortlistReminderCampaign()
    shortlistReminderCampaign.runCampaign(shortlist3rdDayData, recommendationsData, itrSkuSimpleYesterdayData)

  }

  def startSurfCampaigns(campaignsConfig: String) = {

    CampaignManager.initCampaignsConfig(campaignsConfig)

    val yestSurfSessionData = CampaignInput.loadYesterdaySurfSessionData().cache()
    val yestItrSkuData = CampaignInput.loadYesterdayItrSkuData().cache()
    val customerMasterData = loadCustomerMasterData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val yestOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)
    val yestOrderItemData = CampaignInput.loadYesterdayOrderItemData()

    //surf3
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)
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
      brickMvpRecommendations
    )

  }

  def startMiscellaneousCampaigns(campaignsConfig: String) = {
    CampaignManager.initCampaignsConfig(campaignsConfig)
    //loading brickmvp recommendations
    val brickMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()
    //loading brandmvp recommendations
    val brandMvpRecommendations = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_SUB_TYPE).cache()

    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)
    val yesterdaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(1, fullOrderItemData) // created_at
    val itrSkuSimpleYesterdayData = CampaignInput.loadYesterdayItrSimpleData()

    //Start: MIPR email Campaign
    val miprCampaign = new MIPRCampaign()
    miprCampaign.runCampaign(last30DaySalesOrderData, yesterdaySalesOrderItemData, brickMvpRecommendations, itrSkuSimpleYesterdayData)
    val last30DayAcartData = CampaignInput.loadLast30daysAcartData()

    //Start: New Arrival email Campaign
    val newArrivalsBrandCampaign = new NewArrivalsBrandCampaign()
    newArrivalsBrandCampaign.runCampaign(last30DayAcartData, brandMvpRecommendations, itrSkuSimpleYesterdayData)
  }

  def startHottestXCampaign(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val genderMvpBrickRecos = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE)

    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    //FIXME:
    val last60DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(60, fullOrderData, incrDate)
    //FIXME:
    val last60DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(60, fullOrderItemData, incrDate)

    val itrYesterdayData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val hottestXCampaign = new HottestXCampaign

    hottestXCampaign.runCampaign(last60DaySalesOrderData, last60DaySalesOrderItemData, itrYesterdayData, genderMvpBrickRecos, incrDate)

  }

  def startFollowUpCampaigns(params: ParamInfo) = {
    val fullOrderData = CampaignInput.loadFullOrderData()
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))

    //  val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last3DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(3, fullOrderData, incrDate)
    //    val yesterdaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(1, fullOrderItemData) // created_at
    val itrSkYesterdayData = CampaignInput.loadYesterdayItrSkuData()

    val ThirdDayCampaignMergedData = CampaignInput.loadNthDayCampaignMergedData(DataSets.EMAIL_CAMPAIGNS, 3, incrDate)
    //Start: FollowUp email Campaign
    val followUpCampaigns = new FollowUpCampaigns()
    followUpCampaigns.runCampaign(ThirdDayCampaignMergedData, last3DaySalesOrderData, itrSkYesterdayData)
  }

  def startGeoCampaigns(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))

    val fullOrderData = CampaignInput.loadFullOrderData()
    val day40_orderData = CampaignInput.loadNthdayTableData(40, fullOrderData)
    val day50_orderData = CampaignInput.loadNthdayTableData(50, fullOrderData)

    val genderMvpBrickRecos = CampaignInput.loadRecommendationData(Recommendation.BRICK_MVP_SUB_TYPE).cache()
    val genderMvpBrandRecos = CampaignInput.loadRecommendationData(Recommendation.BRAND_MVP_SUB_TYPE).cache()

    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val day40_orderItemData = CampaignInput.loadNthdayTableData(40, fullOrderItemData)
    val day50_orderItemData = CampaignInput.loadNthdayTableData(50, fullOrderItemData)

    val salesAddressData = CampaignInput.loadSalesAddressData()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val cityWiseData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CITY_WISE_DATA, DataSets.FULL_MERGE_MODE, incrDate)

    val geoStyleCampaign = new GeoStyleCampaign
    geoStyleCampaign.runCampaign(day40_orderData, day40_orderItemData, salesAddressData,  yesterdayItrData, cityWiseData, genderMvpBrickRecos)

    val geoBrandCampaign =  new GeoBrandCampaign
    geoBrandCampaign.runCampaign(day50_orderData, day50_orderItemData, salesAddressData,  yesterdayItrData, cityWiseData, genderMvpBrandRecos)

  }

  def startClearanceCampaign(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)

    val fullOrderData = CampaignInput.loadFullOrderData()
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData, incrDate)

    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData, incrDate)

    val mvpDiscountRecos = CampaignInput.loadRecommendationData(Recommendation.MVP_DISCOUNT_SUB_TYPE).cache()

    val yesterdayItrData = CampaignInput.loadYesterdayItrSimpleData().cache()

    val clearanceCampaign = new ClearanceCampaign
    clearanceCampaign.runCampaign(last30DaySalesOrderData, last30DaySalesOrderItemData, mvpDiscountRecos, yesterdayItrData)
  }

  def loadCustomerMasterData(): DataFrame = {

    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day customer master data from hdfs")

    //        val customerMasterData = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, "2015/07/29")
    val customerMasterData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, dateYesterday)
    customerMasterData
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
    require(Array(DataSets.EMAIL_CAMPAIGNS, DataSets.PUSH_CAMPAIGNS) contains campaignType)

    if (CampaignManager.initCampaignsConfigJson(campaignJsonPath)) {
      //      createCampaignMaps(json)
      val saveMode = DataSets.OVERWRITE_SAVEMODE
      val dateFolder = TimeUtils.YESTERDAY_FOLDER
      val allCampaignsData = CampaignInput.loadAllCampaignsData(dateFolder, campaignType)
      val cmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, dateFolder)

      val mergedData =
        if (DataSets.PUSH_CAMPAIGNS == campaignType) {
          val allCamp = CampaignProcessor.mapDeviceFromCMR(cmr, allCampaignsData)
          val itr = CampaignInput.loadYesterdayItrSkuDataForCampaignMerge()
          CampaignProcessor.mergepushCampaigns(allCamp, itr).coalesce(1).cache()
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
      } else {
        val GARBAGE = "NA" //:TODO replace with correct value
        val temp = "temp"
        val expectedDF = mergedData.withColumnRenamed(CampaignMergedFields.LIVE_CART_URL, CampaignMergedFields.LIVE_CART_URL + temp)
          .withColumn(ContactListMobileVars.UID, col(ContactListMobileVars.UID))
          .withColumn(ContactListMobileVars.EMAIL, Udf.addString(col(CampaignMergedFields.EMAIL), lit("**")))
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

          .withColumn(CampaignMergedFields.LIVE_CART_URL, col(CampaignMergedFields.LIVE_CART_URL + temp))
          .withColumn(CampaignMergedFields.LAST_UPDATED_DATE, lit(TimeUtils.yesterday(TimeConstants.DATE_FORMAT)))
          .withColumn(ContactListMobileVars.MOBILE, lit(GARBAGE))
          .withColumn(CampaignMergedFields.TYPO_MOBILE_PERMISION_STATUS, lit(GARBAGE))
          .withColumn(CampaignMergedFields.COUNTRY_CODE, lit(GARBAGE))
          .drop(CustomerVariables.EMAIL)
          .drop(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)
          .drop(CampaignMergedFields.LIVE_CART_URL + temp)

        val emailCampaignFileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_LIVE_CAMPAIGN"
        val csvDataFrame = expectedDF.drop(CampaignMergedFields.CUSTOMER_ID)
          .drop(CampaignMergedFields.REF_SKUS)
          .drop(CampaignMergedFields.REC_SKUS)
        CampaignUtils.debug(expectedDF, "expectedDF final before writing data frame for" + campaignType)
        DataWriter.writeParquet(expectedDF, writePath, saveMode)
        DataWriter.writeCsv(csvDataFrame, DataSets.CAMPAIGNS, DataSets.EMAIL_CAMPAIGNS, DataSets.DAILY_MODE, dateFolder, emailCampaignFileName, saveMode, "true", ";")
      }
    }
  }
}
