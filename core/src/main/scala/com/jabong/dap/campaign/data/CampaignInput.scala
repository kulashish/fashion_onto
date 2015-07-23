package com.jabong.dap.campaign.data

import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.time.{Constants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for providing camapaign input on 15/6/15.
 */
object CampaignInput extends Logging{

  def loadCustomerData(): DataFrame = {
    return null
  }
  
  def loadYesterdayOrderItemData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading last day order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(DataSets.BOB_SOURCE, DataSets.SALES_ORDER_ITEM, "daily", dateYesterday)
    orderItemData
  }

  def loadFullOrderItemData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading full order item data from hdfs")
    val orderItemData = DataReader.getDataFrame(DataSets.BOB_SOURCE, DataSets.SALES_ORDER_ITEM, "full", dateYesterday)
    orderItemData
  }
  
  // based on updated_at
  def loadLastNdaysOrderItemData(n: Int, fullOrderItemData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, Constants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)

    val yesterdayTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, Constants.DATE_TIME_FORMAT_MS))
    val yesterdayEndTime = TimeUtils.getEndTimestampMS(yesterdayTime)

    val lastNdaysOrderItemData = CampaignUtils.getTimeBasedDataFrame(fullOrderItemData, SalesOrderVariables.UPDATED_AT, nDayOldStartTime.toString, yesterdayEndTime.toString)

    lastNdaysOrderItemData
  }

  def loadFullOrderData(): DataFrame = {
    val dateYesterday = TimeUtils.getDateAfterNDays(-1, "yyyy/MM/dd")
    logger.info("Reading full order data from hdfs")
    val orderData = DataReader.getDataFrame(DataSets.BOB_SOURCE, DataSets.SALES_ORDER, "full", dateYesterday)
    orderData
  }

  def loadLastNdaysOrderData(n: Int, fullOrderData: DataFrame): DataFrame = {
    val nDayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-n, Constants.DATE_TIME_FORMAT_MS))
    val nDayOldStartTime = TimeUtils.getStartTimestampMS(nDayOldTime)

    val yesterdayTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, Constants.DATE_TIME_FORMAT_MS))
    val yesterdayEndTime = TimeUtils.getEndTimestampMS(yesterdayTime)

    val lastNdaysOrderData = CampaignUtils.getTimeBasedDataFrame(fullOrderData, SalesOrderVariables.CREATED_AT, nDayOldStartTime.toString, yesterdayEndTime.toString)
    lastNdaysOrderData
  }


  def loadProductData(): DataFrame = {
    return null
  }

}
