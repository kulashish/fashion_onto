package com.jabong.dap.campaign.data

import java.io.File
import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.{DataReader}
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.product.itr.variables.ITR
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for providing camapaign input on 15/6/15.
 */
object CampaignInput extends Logging {

  def loadCustomerData(): DataFrame = {
    return null
  }

  def loadYesterdayOrderItemData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading last day order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB_SOURCE, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, dateYesterday)
    orderItemData
  }

  def loadFullOrderItemData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading full order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB_SOURCE, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, dateYesterday)
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
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading full order data from hdfs")
    val orderData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB_SOURCE, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, dateYesterday)
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
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading last 30 days acart item data from hdfs")

    val acartData = DataReader.getDataFrame(DataSets.INPUT_PATH, DataSets.BOB_SOURCE, DataSets.SALES_CART, DataSets.MONTHLY_MODE, dateYesterday)
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
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading last day basic itr data from hdfs")
    val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, dateYesterday)
    val filteredItr = itrData.select(itrData(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
      itrData(ITR.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE,
      itrData(ITR.QUANTITY) as ProductVariables.STOCK)
    filteredItr
  }

  //FIXME : change to last 30 days
  def loadLast30DaysItrSimpleData() = {
    val thirtyDayOldEndTime = TimeUtils.getDateAfterNDays(-30, "yyyy/MM/dd")
    logger.info("Reading last day basic itr data from hdfs")
    val yesterdayOldEndTime = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    val monthYear = TimeUtils.getMonthAndYear(thirtyDayOldEndTime, "yyyy/MM")
    var itrData:DataFrame = null
    val currentMonthItrData = getCampaignInputDataFrame("orc",DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, monthYear.year+"/"+monthYear.month+"/*")
    val previousMonthItrData = getCampaignInputDataFrame("orc",DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, monthYear.year+"/"+(monthYear.month-1)+"/*")
    if(previousMonthItrData !=null){
      itrData = currentMonthItrData.unionAll(previousMonthItrData)
    }else{
      itrData = currentMonthItrData
    }
    // val itrData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, yesterdayOldEndTime)
    val last30DayOrderData = CampaignUtils.getTimeBasedDataFrame(itrData, SalesOrderVariables.CREATED_AT, thirtyDayOldEndTime.toString, thirtyDayOldEndTime.toString)

    val filteredItr = itrData.select(itrData(ITR.SIMPLE_SKU) as ProductVariables.SKU_SIMPLE,
      itrData(ITR.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE,
      itrData(ITR.QUANTITY) as ProductVariables.STOCK)
    filteredItr
  }

  def loadProductData(): DataFrame = {
    return null
  }

  /**
   * Load all campaign data
   * @return dataframe with call campaigns data
   */
  def loadAllCampaignsData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading last day all campaigns data from hdfs")
    //FIXME:use proper data frame
    // val campaignData = DataReader.getDataFrame(DataSets.OUTPUT_PATH, "campaigns", "*","", dateYesterday)
    val campaignData = Spark.getSqlContext().read.parquet("/data/output/campaigns/*/2015/07/26/")
    val allCampaignData = campaignData.select(
      campaignData(CustomerVariables.FK_CUSTOMER) as (CampaignMergedFields.FK_CUSTOMER),
      campaignData(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
      campaignData(CampaignMergedFields.REF_SKU1))

    return allCampaignData
  }


  def getCampaignInputDataFrame(fileFormat:String,basePath: String, source: String, componentName: String, mode: String, date: String): DataFrame ={
    val filePath = buildPath(basePath, source, componentName, mode, date)
    var loadedDataframe:DataFrame = null
    if(fileFormat == "orc"){
      if(DataVerifier.dataExists(filePath)){
        loadedDataframe = Spark.getHiveContext().read.format(fileFormat).load(filePath)
      }else{
        return null
      }
    }
    if(fileFormat == "parquet") {
      if (DataVerifier.dataExists(filePath)) {
        loadedDataframe = Spark.getSqlContext().read.format(fileFormat).load(filePath)
      } else {
        return null
      }
    }
      return loadedDataframe
  }

  def buildPath(basePath: String, source: String, componentName: String, mode: String, date: String): String = {
    //here if Date has "-", it will get changed to File.separator.
    "%s/%s/%s/%s/%s".format(basePath, source, componentName, mode, date.replaceAll("-", File.separator))
  }
}
