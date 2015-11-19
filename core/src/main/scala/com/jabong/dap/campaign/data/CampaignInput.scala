package com.jabong.dap.campaign.data

import java.io.File
import java.sql.Timestamp

import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.CampaignInfo
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.clickstream.ClickStreamConstant
import com.jabong.dap.model.product.itr.variables.ITR
import grizzled.slf4j.Logging
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul for providing camapaign input on 15/6/15.
 */
object CampaignInput extends Logging {

  val SALES_HOUR_DIFF = 1

  def readCustomerData(path: String, date: String): DataFrame = {

    return null

  }

  def loadCustomerData(): DataFrame = {
    return null
  }

  def loadYesterdaySurfSessionData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day surf session data from hdfs")

    val surfSessionData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf1ProcessedVariable", DataSets.DAILY_MODE, dateYesterday)
    surfSessionData
  }

  def loadLastDaySurf3Data(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day surf 3 data from hdfs")

    val surf3Data = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3ProcessedVariable", DataSets.DAILY_MODE, dateYesterday)
    surf3Data
  }

  def loadCampaignOutput(date: String): DataFrame = {
    return null
  }

  def loadCustomerMasterData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day customer master data from hdfs")

    val customerMasterData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, dateYesterday)
    customerMasterData
  }

  def loadYesterdayOrderItemData() = loadOrderItemData()

  def loadOrderItemData(date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    logger.info("Reading order item data from hdfs for " + date)
    val orderItemData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, date)
    orderItemData
  }

  def loadFullOrderItemData(date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    //  val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading full order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, date)
    orderItemData
  }

  // based on updated_at
  def loadLastNdaysOrderItemData(n: Int, fullOrderItemData: DataFrame, date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    val dateTimeMs = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_TIME_FORMAT_MS)

    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS, dateTimeMs))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)

    val dateTime = Timestamp.valueOf(dateTimeMs)
    val dateEndTime = TimeUtils.getEndTimestampMS(dateTime)

    val lastNdaysOrderItemData = Utils.getTimeBasedDataFrame(fullOrderItemData, SalesOrderVariables.UPDATED_AT, nDayOldStartTime.toString, dateEndTime.toString)

    lastNdaysOrderItemData
  }

  def loadFullOrderData(date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    //val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading full order data from hdfs")
    val orderData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, date)
    orderData
  }

  def loadLastNdaysOrderData(n: Int, fullOrderData: DataFrame, date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    val dateTimeMs = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_TIME_FORMAT_MS)
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS, dateTimeMs))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)

    val dateTime = Timestamp.valueOf(dateTimeMs)
    val dateEndTime = TimeUtils.getEndTimestampMS(dateTime)

    val lastNdaysOrderData = Utils.getTimeBasedDataFrame(fullOrderData, SalesOrderVariables.CREATED_AT, nDayOldStartTime.toString, dateEndTime.toString)
    lastNdaysOrderData
  }

  def loadLast30daysAcartData(date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    logger.info("Reading last 30 days acart item data from hdfs")

    val acartData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_CART, DataSets.MONTHLY_MODE, date)
    acartData
  }

  // 1 day data only
  def loadNthdayAcartData(n: Int, last30daysAcartData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)
    val nDayOldEndTime = TimeUtils.getEndTimestampMS(nDayOldTime)

    val nthDayOrderData = Utils.getTimeBasedDataFrame(last30daysAcartData, SalesOrderVariables.CREATED_AT, nDayOldStartTime.toString, nDayOldEndTime.toString)
    nthDayOrderData
  }

  /**
   *
   * @param n
   * @param inputData
   * @return
   */
  def loadNthdayTableData(n: Int, inputData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)
    val nDayOldEndTime = TimeUtils.getEndTimestampMS(nDayOldTime)

    val nthDayOrderData = Utils.getTimeBasedDataFrame(inputData, SalesOrderVariables.CREATED_AT, nDayOldStartTime.toString, nDayOldEndTime.toString)
    nthDayOrderData
  }

  /**
   * load yesterdays itr sku simle data
   * @param dateYesterday
   * @return
   */
  def loadYesterdayItrSimpleData(dateYesterday: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)) = {
    logger.info("Reading last day basic itr simple data from hdfs")
    val itrData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, dateYesterday)

    val filteredItr = itrData.select(itrData(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
      itrData(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
      itrData(ITR.QUANTITY) as ProductVariables.STOCK,
      itrData(ITR.MVP) as ProductVariables.MVP,
      itrData(ITR.GENDER) as ProductVariables.GENDER,
      itrData(ITR.BRAND_NAME) as ProductVariables.BRAND,
      itrData(ITR.BRICK) as ProductVariables.BRICK,
      itrData(ITR.PRODUCT_NAME),
      itrData(ITR.COLOR),
      itrData(ITR.PRICE_BAND),
      itrData(ITR.ACTIVATED_AT) as ProductVariables.ACTIVATED_AT,
      itrData(ITR.ITR_DATE) as ItrVariables.CREATED_AT,
      itrData(ITR.REPORTING_CATEGORY) as ProductVariables.CATEGORY)

    filteredItr
  }

  def loadYesterdayItrSkuData(date: String = TimeUtils.YESTERDAY_FOLDER) = {
    //val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day basic itr sku data from hdfs")
    val itrData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)

    val filteredItr = itrData.select(itrData(ITR.CONFIG_SKU) as ProductVariables.SKU,
      itrData(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
      itrData(ITR.QUANTITY) as ProductVariables.STOCK,
      itrData(ITR.MVP) as ProductVariables.MVP,
      itrData(ITR.GENDER) as ProductVariables.GENDER,
      itrData(ITR.BRAND_NAME) as ProductVariables.BRAND,
      itrData(ITR.PRICE_BAND),
      itrData(ITR.COLOR),
      itrData(ITR.DISCOUNT),
      itrData(ITR.ITR_DATE) as ItrVariables.CREATED_AT,
      itrData(ITR.BRICK),
      itrData(ITR.PRODUCT_NAME),
      itrData(ITR.NUMBER_SIMPLE_PER_SKU) as ProductVariables.NUMBER_SIMPLE_PER_SKU,
      itrData(ITR.REPORTING_CATEGORY) as ProductVariables.CATEGORY)

    filteredItr
  }

  def loadYesterdayItrSkuDataForCampaignMerge(date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    //val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day basic itr sku data from hdfs")
    val itrData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)

    val filteredItr = itrData.select(itrData(ITR.CONFIG_SKU),
      itrData(ITR.BRAND_NAME),
      itrData(ITR.PRODUCT_NAME),
      itrData(ITR.BRICK))
    filteredItr
  }

  /*
  //FIXME : change to last 30 days
  def loadLast30DaysItrSimpleData() = {
    val thirtyDayOldEndTime = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last 30 days basic itr data from hdfs")
    val yesterdayOldEndTime = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    val monthYear = TimeUtils.getMonthAndYear(yesterdayOldEndTime, TimeConstants.DATE_FORMAT_FOLDER)
    val monthStr = TimeUtils.withLeadingZeros(monthYear.month + 1)
    val monthPrevStr = TimeUtils.withLeadingZeros(monthYear.month)

    var itrData: DataFrame = null
    val currentMonthItrData = getCampaignInputDataFrame(DataSets.ORC, ConfigConstants.OUTPUT_PATH, "itr", "basic", "", monthYear.year + File.separator + monthStr)
    val previousMonthItrData = getCampaignInputDataFrame(DataSets.ORC, ConfigConstants.OUTPUT_PATH, "itr", "basic", "", monthYear.year + File.separator + monthPrevStr)
    if (previousMonthItrData != null) {
      itrData = currentMonthItrData.unionAll(previousMonthItrData)
    } else {
      itrData = currentMonthItrData
    }

    println("COUNT " + itrData.count)
    // val itrData = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, yesterdayOldEndTime)
    val last30DayItrData = CampaignUtils.getTimeBasedDataFrame(itrData, ITR.ITR_DATE, yesterdayOldEndTime.toString, thirtyDayOldEndTime.toString)

    val filteredItr = last30DayItrData.select(last30DayItrData(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
      last30DayItrData(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
      last30DayItrData(ITR.QUANTITY) as ProductVariables.STOCK)
    last30DayItrData(ITR.ITR_DATE) as ItrVariables.CREATED_AT
    filteredItr
  }*/

  def loadFullShortlistData(date: String = TimeUtils.YESTERDAY_FOLDER) = {
    //val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)
    val dateDiffFormat = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    logger.info("Reading full fetch shortlist data from hdfs")
    val shortlistData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_PRODUCT_SHORTLIST, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    shortlistData
  }

  def loadProductData(): DataFrame = {
    return null
  }

  /**
   * Load all campaign data
   * @return dataframe with call campaigns data
   */
  def loadAllCampaignsData(date: String, campaignType: String): DataFrame = {
    require(Array(DataSets.EMAIL_CAMPAIGNS, DataSets.PUSH_CAMPAIGNS,DataSets.CALENDAR_CAMPAIGNS) contains campaignType)

    logger.info("Reading last day all campaigns data from hdfs : CampaignType" + campaignType)
    //FIXME:use proper data frame
    var allCampaignData: DataFrame = null
    var df: DataFrame = null
    for (campaignDetails <- CampaignInfo.campaigns.pushCampaignList) {

      val campaignPriority = campaignDetails.priority
      val campaignName = campaignDetails.campaignName

      df = getCampaignData(campaignName, date, campaignType, campaignPriority)
      if (null != allCampaignData && null != df) allCampaignData = allCampaignData.unionAll(df) else if (null == allCampaignData) allCampaignData = df
    }
    logger.info("merging full campaign done for type: " + campaignType)
    return allCampaignData
  }

  /**
   * get campaign data for particular with priority
   * @param name
   * @param date
   * @param priority
   * @return
   */
  def getCampaignData(name: String, date: String, campaignType: String, priority: Int = CampaignCommon.VERY_LOW_PRIORITY): DataFrame = {
    require(Array(DataSets.EMAIL_CAMPAIGNS, DataSets.PUSH_CAMPAIGNS) contains campaignType)

    val path: String = ConfigConstants.READ_OUTPUT_PATH + File.separator + campaignType + File.separator + name + File.separator + DataSets.DAILY_MODE + File.separator + date
    logger.info(" Reading " + name + " campaign data from path:- " + path + ", Type: " + campaignType)
    if (DataVerifier.dataExists(path)) {
      var result: DataFrame = null
      try {
        val campaignData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, campaignType, name, DataSets.DAILY_MODE, date)
          .withColumn(CampaignCommon.PRIORITY, lit(priority))

        val campaignSchema = if (DataSets.PUSH_CAMPAIGNS == campaignType) Schema.campaignSchema else Schema.emailCampaignSchema

        if (!SchemaUtils.isSchemaEqual(campaignData.schema, campaignSchema)) {
          val res = SchemaUtils.addColumns(campaignData, campaignSchema)
          if (DataSets.PUSH_CAMPAIGNS == campaignType) {
            result = res
              .select(
                res(CustomerVariables.FK_CUSTOMER) as (CampaignMergedFields.CUSTOMER_ID),
                res(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
                res(CampaignMergedFields.REF_SKU1),
                res(CampaignMergedFields.EMAIL),
                res(CampaignMergedFields.DOMAIN),
                res(CampaignMergedFields.DEVICE_ID),
                res(CampaignCommon.PRIORITY)
              )
          } else {
            result = res.select(
              res(CustomerVariables.EMAIL),
              res(CampaignMergedFields.REF_SKUS),
              res(CampaignMergedFields.REC_SKUS),
              res(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
              res(CampaignCommon.PRIORITY),
              res(CampaignMergedFields.LIVE_CART_URL)
            )
          }
        } else {
          logger.info("Adding campaign data to allCampaigns without changing the schema for type: " + campaignType)
          result = campaignData
        }
      } catch {
        // TODO: fix when data not found skip
        case th: Throwable => {
          logger.info("File Not found at ->" + path)
          throw new SparkException("Data not available ?", th)
        }
      }
      logger.info("Before replacing null customer id with 0 and device_id with empty string: ")

      if (DataSets.PUSH_CAMPAIGNS == campaignType)
        return result.na.fill(Map(CampaignMergedFields.CUSTOMER_ID -> 0, CampaignMergedFields.DEVICE_ID -> ""))
      else return result
    }
    return null
  }

  def getCampaignInputDataFrame(fileFormat: String, basePath: String, source: String, componentName: String, mode: String, date: String): DataFrame = {
    val filePath = buildPath(basePath, source, componentName, mode, date)
    var loadedDataframe: DataFrame = null
    logger.info(" orc data loaded from filepath" + filePath)
    //FIXME Compress the below if else loop.
    if (fileFormat == DataSets.ORC) {

      if (DataVerifier.dirExists(filePath)) {
        loadedDataframe = Spark.getHiveContext().read.format(fileFormat).load(filePath + "/*")
        logger.info(" orc data loaded from filepath" + filePath)
      } else {
        return null
      }
    }
    if (fileFormat == DataSets.PARQUET) {
      if (DataVerifier.dirExists(filePath)) {
        loadedDataframe = Spark.getSqlContext().read.format(fileFormat).load(filePath + "/*")
        logger.info(" parquet data loaded from filepath" + filePath)

      } else {
        return null
      }
    }

    return loadedDataframe
  }

  def buildPath(basePath: String, source: String, componentName: String, mode: String, date: String): String = {
    //here if Date has "-", it will get changed to File.separator.
    logger.info("PATH IS " + "%s/%s/%s/%s/%s".format(basePath, source, componentName, mode, date.replaceAll("-", File.separator)))
    "%s/%s/%s/%s/%s".format(basePath, source, componentName, mode, date.replaceAll("-", File.separator))
  }

  def load30DayItrSkuData(date: String = TimeUtils.YESTERDAY_FOLDER) = {

    //var date = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    var itr30Day = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)
      .select(
        col(ITR.CONFIG_SKU) as ProductVariables.SKU,
        col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
        col(ITR.QUANTITY) as ProductVariables.STOCK,
        col(ITR.MVP) as ProductVariables.MVP,
        col(ITR.GENDER) as ProductVariables.GENDER,
        col(ITR.BRAND_NAME) as ProductVariables.BRAND,
        col(ITR.PRICE_BAND),
        col(ITR.ITR_DATE) as ItrVariables.CREATED_AT,
        col(ITR.BRICK),
        col(ITR.PRODUCT_NAME),
        col(ITR.NUMBER_SIMPLE_PER_SKU) as ProductVariables.NUMBER_SIMPLE_PER_SKU,
        col(ITR.REPORTING_CATEGORY) as ProductVariables.CATEGORY)

    for (i <- 2 to 30) {

      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      logger.info("Reading last " + i + " day basic itr sku data from hdfs")

      val path = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)
      val itrExits = DataVerifier.dataExists(path)

      if (itrExits) {
        val itrData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)
        itr30Day = itr30Day.unionAll(itrData.select(
          col(ITR.CONFIG_SKU) as ProductVariables.SKU,
          col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
          col(ITR.QUANTITY) as ProductVariables.STOCK,
          col(ITR.MVP) as ProductVariables.MVP,
          col(ITR.GENDER) as ProductVariables.GENDER,
          col(ITR.BRAND_NAME) as ProductVariables.BRAND,
          col(ITR.PRICE_BAND),
          col(ITR.ITR_DATE) as ItrVariables.CREATED_AT,
          col(ITR.BRICK),
          col(ITR.PRODUCT_NAME),
          col(ITR.NUMBER_SIMPLE_PER_SKU) as ProductVariables.NUMBER_SIMPLE_PER_SKU,
          col(ITR.REPORTING_CATEGORY) as ProductVariables.CATEGORY))
      }
    }
    itr30Day
  }

  def load30DayItrSkuSimpleData(date: String = TimeUtils.YESTERDAY_FOLDER) = {

    //var date = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    var itr30Day = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, date)
      .select(
        col(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
        col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
        col(ITR.MVP) as ProductVariables.MVP,
        col(ITR.GENDER) as ProductVariables.GENDER,
        col(ITR.BRAND_NAME) as ProductVariables.BRAND,
        col(ITR.BRICK) as ProductVariables.BRICK,
        col(ITR.PRICE_BAND),
        col(ITR.PRODUCT_NAME),
        col(ITR.ACTIVATED_AT) as ProductVariables.ACTIVATED_AT,
        col(ITR.ITR_DATE) as CustomerProductShortlistVariables.CREATED_AT)

    for (i <- 2 to 30) {

      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      logger.info("Reading last " + i + " day basic itr sku simple data from hdfs")

      val path = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, date)
      val itrExits = DataVerifier.dataExists(path)

      if (itrExits) {
        logger.info("Adding last " + i + " day basic itr sku simple data from hdfs")
        val itrData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, date)
        itr30Day = itr30Day.unionAll(itrData.select(
          col(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
          col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
          col(ITR.MVP) as ProductVariables.MVP,
          col(ITR.GENDER) as ProductVariables.GENDER,
          col(ITR.BRAND_NAME) as ProductVariables.BRAND,
          col(ITR.BRICK) as ProductVariables.BRICK,
          col(ITR.PRICE_BAND),
          col(ITR.PRODUCT_NAME),
          col(ITR.ACTIVATED_AT) as ProductVariables.ACTIVATED_AT,
          col(ITR.ITR_DATE) as CustomerProductShortlistVariables.CREATED_AT))
      }
    }
    itr30Day
  }

  def load30DayCampaignMergedData(campaignType: String = DataSets.PUSH_CAMPAIGNS): DataFrame = {

    var campaignMerged30Day: DataFrame = null

    for (i <- 2 to 31) {

      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)

      logger.info("Reading last " + i + " day basic campaign Merged datafrom hdfs")

      val mergedCampaignData = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, campaignType, "merged", DataSets.DAILY_MODE, date)
      if (null != mergedCampaignData) {
        if (campaignMerged30Day == null) {
          campaignMerged30Day = mergedCampaignData
        } else {
          var newMergedCamapignData = mergedCampaignData
          if (!SchemaUtils.isSchemaEqual(mergedCampaignData.schema, campaignMerged30Day.schema)) {
            // added to add new column add4pushId for the old camaigns data
            newMergedCamapignData = SchemaUtils.addColumns(mergedCampaignData, campaignMerged30Day.schema)
          }
          campaignMerged30Day = campaignMerged30Day.unionAll(newMergedCamapignData)
        }
      }
    }
    campaignMerged30Day
  }

  /**
   * to get campaign data for a particular date
   * @param campaignType
   * @param nDays
   * @return
   */
  def loadNthDayCampaignMergedData(campaignType: String = DataSets.PUSH_CAMPAIGNS, nDays: Int = -1, incrDate: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    val date = TimeUtils.getDateAfterNDays(-nDays, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
    val mergedCampaignData = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, campaignType, "merged", DataSets.DAILY_MODE, date)

    mergedCampaignData
  }

  def loadYesterdayMobilePushCampaignQualityData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day Mobile Push Campaign Quality data from hdfs")
    val mobilePushCampaignQuality = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CAMPAIGNS, CampaignCommon.MOBILE_PUSH_CAMPAIGN_QUALITY, DataSets.DAILY_MODE, dateYesterday)
    mobilePushCampaignQuality
  }

  /**
   * Load recommendation Data
   * @param recommendationType
   * @param date
   * @return
   */
  def loadRecommendationData(recommendationType: String, date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    logger.info("Reading recommendation for recommendation type %s and for date %s", recommendationType, date)
    val recommendations = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RECOMMENDATIONS, recommendationType, DataSets.DAILY_MODE, date)
    recommendations
  }

  /**
   *
   * @param fullShortlistData
   * @param ndays
   * @return
   */
  def loadNthDayShortlistData(fullShortlistData: DataFrame, ndays: Int, todayDate: String): DataFrame = {

    if (fullShortlistData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    if (ndays <= 0) {

      logger.error("ndays should not be negative value")

      return null

    }

    val timestamp = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-ndays, TimeConstants.DATE_TIME_FORMAT_MS, todayDate))
    val startTimestamp = TimeUtils.getStartTimestampMS(timestamp)
    val endTimestamp = TimeUtils.getEndTimestampMS(timestamp)

    val nthDayShortlistData = Utils.getTimeBasedDataFrame(fullShortlistData, CustomerProductShortlistVariables.CREATED_AT, startTimestamp.toString, endTimestamp.toString)

    return nthDayShortlistData
  }

  /**
   *
   * @param fullShortlistData
   * @param ndays
   * @return
   */
  def loadNDaysShortlistData(fullShortlistData: DataFrame, ndays: Int, todayDate: String): DataFrame = {

    if (fullShortlistData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    if (ndays <= 0) {

      logger.error("ndays should not be negative value")

      return null

    }

    val dateBeforeNdays = TimeUtils.getDateAfterNDays(-ndays, TimeConstants.DATE_TIME_FORMAT_MS, todayDate)
    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT_MS, todayDate)

    val startTimestamp = TimeUtils.getStartTimestampMS(Timestamp.valueOf(dateBeforeNdays))
    val endTimestamp = TimeUtils.getEndTimestampMS(Timestamp.valueOf(yesterdayDate))

    val nDaysShortlistData = Utils.getTimeBasedDataFrame(fullShortlistData, CustomerProductShortlistVariables.CREATED_AT, startTimestamp.toString, endTimestamp.toString)

    return nDaysShortlistData
  }

  /**
   *
   * @param tableName
   * @param lastHour
   * @return
   */
  def loadNthHourTableData(tableName: String, lastHour: Int, date: String): DataFrame = {
    val incrDateHour: String = TimeUtils.getDateAfterNHours(lastHour, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER, date)
    val tableData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, tableName, DataSets.HOURLY_MODE, incrDateHour)
    return tableData
  }

  /**
   *
   * @param tableName
   * @param lastHour
   * @return
   */
  def loadNHoursTableData(tableName: String, lastHour: Int, date: String): DataFrame = {

    var tableNameUnionData: DataFrame = null

    for (i <- lastHour to -1) {

      val incrDateHour: String = TimeUtils.getDateAfterNHours(i, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER, date)

      logger.info("Reading last " + i + " hours " + tableName + " data from hdfs")

      val tableNameData = DataReader.getDataFrameOrNull(ConfigConstants.INPUT_PATH, DataSets.BOB, tableName, DataSets.HOURLY_MODE, incrDateHour)
      if (null != tableNameData) {
        if (tableNameUnionData == null) {
          tableNameUnionData = tableNameData
        } else {
          tableNameUnionData = tableNameUnionData.unionAll(tableNameData)
        }
      }
    }
    tableNameUnionData
  }

  /**
   *
   * @param campaignName
   * @param campaignType
   * @param date
   * @return
   */
  def loadNHoursCampaignData(campaignName: String, campaignType: String, date: String = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)): DataFrame = {

    var tableNameUnionData: DataFrame = null

    val lastHour = TimeUtils.getHour(date, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER)

    for (i <- lastHour to 1 by -1) {

      val incrDateHour: String = TimeUtils.getDateAfterNHours(i, TimeConstants.DATE_TIME_FORMAT_HRS_FOLDER, date)

      logger.info("Reading last " + i + " hours " + campaignName + "data from hdfs")

      val tableNameData = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, campaignType, campaignName, DataSets.HOURLY_MODE, incrDateHour)
      if (null != tableNameData) {
        if (tableNameUnionData == null) {
          tableNameUnionData = tableNameData
        } else {
          tableNameUnionData = tableNameUnionData.unionAll(tableNameData)
        }
      }
    }
    tableNameUnionData
  }

  def loadFullVariablesData(tableName: String, date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {
    logger.info("Reading Full" + tableName + "Data data from hdfs")
    val orderData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, tableName, DataSets.FULL_MERGE_MODE, date)
    orderData
  }

  def loadPageViewSurfData(date: String = TimeUtils.YESTERDAY_FOLDER): DataFrame = {

    val hiveContext = Spark.getHiveContext()
    val monthYear = TimeUtils.getMonthAndYear(date, TimeConstants.DATE_FORMAT_FOLDER)
    val month = monthYear.month + 1
    val day = monthYear.day
    val year = monthYear.year

    val hiveQuery = "SELECT userid, productsku,pagets,sessionid FROM " + ClickStreamConstant.MERGE_PAGEVISIT +
      " where userid is not null and pagetype in ('CPD','QPD','DPD') and pagets is not null and sessionid is not null and sessionid != '(null)' and " +
      "date1 = " + day + " and month1 = " + month + " and year1=" + year

    val pageViewSurfData = hiveContext.sql(hiveQuery)
    return pageViewSurfData
  }
}
