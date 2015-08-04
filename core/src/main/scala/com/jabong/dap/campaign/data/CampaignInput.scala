package com.jabong.dap.campaign.data

import java.io.File
import java.sql.Timestamp

import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.read.{ DataReader, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.product.itr.variables.ITR
import grizzled.slf4j.Logging
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul for providing camapaign input on 15/6/15.
 */
object CampaignInput extends Logging {

  def readCustomerData(path: String, date: String): DataFrame = {

    return null

  }

  def loadCustomerData(): DataFrame = {
    return null
  }

  def loadYesterdaySurfSessionData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day surf session data from hdfs")

    val surfSessionData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "clickstream", "Surf1ProcessedVariable", DataSets.DAILY_MODE, dateYesterday)
    surfSessionData
  }

  def loadLastDaySurf3Data(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day surf 3 data from hdfs")

    val surf3Data = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3ProcessedVariable", DataSets.DAILY_MODE, dateYesterday)
    surf3Data
  }

  def loadCampaignOutput(date: String): DataFrame = {
    return null
  }

  def loadCustomerMasterData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day customer master data from hdfs")

    val customerMasterData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, dateYesterday)
    customerMasterData
  }

  def loadYesterdayOrderItemData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, dateYesterday)
    orderItemData
  }

  def loadFullOrderItemData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading full order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, dateYesterday)
    orderItemData
  }

  // based on updated_at
  def loadLastNdaysOrderItemData(n: Int, fullOrderItemData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)

    val yesterdayTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT_MS))
    val yesterdayEndTime = TimeUtils.getEndTimestampMS(yesterdayTime)

    val lastNdaysOrderItemData = CampaignUtils.getTimeBasedDataFrame(fullOrderItemData, SalesOrderVariables.UPDATED_AT, nDayOldStartTime.toString, yesterdayEndTime.toString)

    lastNdaysOrderItemData
  }

  def loadFullOrderData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading full order data from hdfs")
    val orderData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, dateYesterday)
    orderData
  }

  def loadLastNdaysOrderData(n: Int, fullOrderData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)

    val yesterdayTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT_MS))
    val yesterdayEndTime = TimeUtils.getEndTimestampMS(yesterdayTime)

    val lastNdaysOrderData = CampaignUtils.getTimeBasedDataFrame(fullOrderData, SalesOrderVariables.CREATED_AT, nDayOldStartTime.toString, yesterdayEndTime.toString)
    lastNdaysOrderData
  }

  def loadLast30daysAcartData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last 30 days acart item data from hdfs")

    val acartData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.SALES_CART, DataSets.MONTHLY_MODE, dateYesterday)
    acartData
  }

  // 1 day data only
  def loadNthdayAcartData(n: Int, last30daysAcartData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, TimeConstants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)
    val nDayOldEndTime = TimeUtils.getEndTimestampMS(nDayOldTime)

    val nthDayOrderData = CampaignUtils.getTimeBasedDataFrame(last30daysAcartData, SalesOrderVariables.CREATED_AT, nDayOldStartTime.toString, nDayOldEndTime.toString)
    nthDayOrderData
  }

  def loadYesterdayItrSimpleData() = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day basic itr simple data from hdfs")
    val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, dateYesterday)
    val filteredItr = itrData.select(itrData(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
      itrData(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
      itrData(ITR.QUANTITY) as ProductVariables.STOCK,
      itrData(ITR.ITR_DATE) as ItrVariables.CREATED_AT)

    filteredItr
  }

  def loadYesterdayItrSkuData() = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day basic itr sku data from hdfs")
    val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, dateYesterday)
    val filteredItr = itrData.select(itrData(ITR.CONFIG_SKU) as ProductVariables.SKU,
      itrData(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
      itrData(ITR.QUANTITY) as ProductVariables.STOCK,
      itrData(ITR.ITR_DATE) as ItrVariables.CREATED_AT,
      itrData(ITR.BRICK))
    filteredItr
  }

  def loadYesterdayItrSkuDataForCampaignMerge(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    logger.info("Reading last day basic itr sku data from hdfs")
    val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, dateYesterday)
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
    val currentMonthItrData = getCampaignInputDataFrame("orc", DataSets.OUTPUT_PATH, "itr", "basic", "", monthYear.year + "/" + monthStr)
    val previousMonthItrData = getCampaignInputDataFrame("orc", DataSets.OUTPUT_PATH, "itr", "basic", "", monthYear.year + "/" + monthPrevStr)
    if (previousMonthItrData != null) {
      itrData = currentMonthItrData.unionAll(previousMonthItrData)
    } else {
      itrData = currentMonthItrData
    }

    println("COUNT " + itrData.count)
    // val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, yesterdayOldEndTime)
    val last30DayItrData = CampaignUtils.getTimeBasedDataFrame(itrData, ITR.ITR_DATE, yesterdayOldEndTime.toString, thirtyDayOldEndTime.toString)

    val filteredItr = last30DayItrData.select(last30DayItrData(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
      last30DayItrData(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
      last30DayItrData(ITR.QUANTITY) as ProductVariables.STOCK)
    last30DayItrData(ITR.ITR_DATE) as ItrVariables.CREATED_AT
    filteredItr
  }*/

  def loadFullShortlistData() = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)
    logger.info("Reading full fetch shortlist data from hdfs")
    val shortlistData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_PRODUCT_SHORTLIST, DataSets.FULL_FETCH_MODE, dateYesterday)
    shortlistData
  }

  def loadProductData(): DataFrame = {
    return null
  }

  /**
   * Load all campaign data
   * @return dataframe with call campaigns data
   */
  def loadAllCampaignsData(date: String): DataFrame = {
    logger.info("Reading last day all campaigns data from hdfs")
    //FIXME:use proper data frame
    var allCampaignData: DataFrame = null
    var df: DataFrame = null
    CampaignManager.campaignMailTypeMap.foreach (
      e => (
        if (null == allCampaignData) {
          val campaignPriority = OptionUtils.getOptIntVal(CampaignManager.mailTypePriorityMap.get(e._2), CampaignCommon.VERY_LOW_PRIORITY)
          df = getCampaignData(e._1, date, campaignPriority)
          if (null != df) {
            allCampaignData = df
          }
        } else {
          val campaignPriority = OptionUtils.getOptIntVal(CampaignManager.mailTypePriorityMap.get(e._2), CampaignCommon.VERY_LOW_PRIORITY)
          df = getCampaignData(e._1, date, campaignPriority)
          if (null != df) {
            allCampaignData = allCampaignData.unionAll(df)
          }
        }))
    println("merging full campaign done")
    return allCampaignData.dropDuplicates()
  }

  def getCampaignData(name: String, date: String, priority: Int): DataFrame = {
    var path: String = DataSets.OUTPUT_PATH + "/" + DataSets.CAMPAIGN + "/" + name + "/" + DataSets.DAILY_MODE + "/" + date
    if (DataVerifier.dataExists(path)) {
      try {
        val campaignData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.CAMPAIGN, name, DataSets.DAILY_MODE, date)
        if (!SchemaUtils.isSchemaEqual(campaignData.schema, Schema.campaignSchema)) {
          val res = SchemaUtils.changeSchema(campaignData, Schema.campaignSchema).withColumn(CampaignCommon.PRIORITY, lit(priority))
          return res
            .select(
              res(CustomerVariables.FK_CUSTOMER) as (CampaignMergedFields.CUSTOMER_ID),
              res(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
              res(CampaignMergedFields.REF_SKU1),
              res(CampaignMergedFields.EMAIL),
              res(CampaignMergedFields.DOMAIN),
              res(CampaignMergedFields.DEVICE_ID),
              res(CampaignCommon.PRIORITY)
            )
            .na.fill(
              Map(
                CampaignMergedFields.CUSTOMER_ID -> 0,
                CampaignMergedFields.DEVICE_ID -> ""
              )
            )
        }
        println("Adding campaign data to allCampaigns: ") // + campaignData.count())
        //campaignData.printSchema()
        //campaignData.show(9)
        campaignData
      } catch {
        // TODO: fix when data not found skip
        case th: Throwable => {
          logger.info("File Not found at ->" + path)
          throw new SparkException("Data not available ?", th)
        }
      }
    } else {
      return null
    }
  }

  def getCampaignInputDataFrame(fileFormat: String, basePath: String, source: String, componentName: String, mode: String, date: String): DataFrame = {
    val filePath = buildPath(basePath, source, componentName, mode, date)
    var loadedDataframe: DataFrame = null
    logger.info(" orc data loaded from filepath" + filePath)
    //FIXME Compress the below if else loop.
    if (fileFormat == "orc") {

      if (DataVerifier.dirExists(filePath)) {
        loadedDataframe = Spark.getHiveContext().read.format(fileFormat).load(filePath + "/*")
        logger.info(" orc data loaded from filepath" + filePath)
      } else {
        return null
      }
    }
    if (fileFormat == "parquet") {
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
    println("PATH IS " + "%s/%s/%s/%s/%s".format(basePath, source, componentName, mode, date.replaceAll("-", File.separator)))
    "%s/%s/%s/%s/%s".format(basePath, source, componentName, mode, date.replaceAll("-", File.separator))
  }

  def load30DayItrSkuData() = {

    var date = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val itr30Day = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)
      .select(
        col(ITR.CONFIG_SKU) as ProductVariables.SKU,
        col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
        col(ITR.ITR_DATE) as CustomerProductShortlistVariables.CREATED_AT)

    for (i <- 2 to 30) {

      date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      logger.info("Reading last " + i + " day basic itr sku data from hdfs")

      val path = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)
      val itrExits = DataVerifier.dataExists(path)

      if (itrExits) {
        val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, date)
        itr30Day.unionAll(itrData.select(
          col(ITR.CONFIG_SKU) as ProductVariables.SKU,
          col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
          col(ITR.ITR_DATE) as CustomerProductShortlistVariables.CREATED_AT))
      }
    }

    itr30Day
  }

  def load30DayItrSkuSimpleData() = {

    var date = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val itr30Day = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, date)
      .select(
        col(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
        col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
        col(ITR.ITR_DATE) as CustomerProductShortlistVariables.CREATED_AT)

    for (i <- 2 to 30) {

      date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      logger.info("Reading last " + i + " day basic itr sku data from hdfs")

      val path = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, date)
      val itrExits = DataVerifier.dataExists(path)

      if (itrExits) {
        val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, date)
        itr30Day.unionAll(itrData.select(
          col(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
          col(ITR.PRICE_ON_SITE) as ProductVariables.SPECIAL_PRICE,
          col(ITR.ITR_DATE) as CustomerProductShortlistVariables.CREATED_AT))
      }
    }

    itr30Day
  }

  def load30DayCampaignMergedData(): DataFrame = {

    var campaignMerged30Day: DataFrame = null

    for (i <- 1 to 30) {

      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)

      logger.info("Reading last " + i + " day basic campaign Merged datafrom hdfs")

      val path = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "campaigns", "merged", DataSets.DAILY_MODE, date)
      val campaignMergedExits = DataVerifier.dataExists(path)

      if (campaignMergedExits) {
        val mergedCampaignData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "campaigns", "merged", DataSets.DAILY_MODE, date)
        if (campaignMerged30Day == null) {
          campaignMerged30Day = mergedCampaignData
        } else {
          campaignMerged30Day.unionAll(mergedCampaignData)
        }
      }
    }
    campaignMerged30Day
  }

}
