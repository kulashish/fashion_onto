package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by Kapil.Rajak on 14/8/15.
 */
object CampaignQualityEntry extends Logging {

  var orderItemFullData, orderItemData, orderItem30DaysData, orderItem3DaysData, fullOrderData, last30DaysOrderData, salesCart30DaysData, salesCart3rdDayData, yestSessionData: DataFrame = null

  def start(paramInfo: ParamInfo) = {
    val DEFAULT_FRACTION = ".15"

    logger.info("Campaign Quality Triggered")
    val incrDate = OptionUtils.getOptValue(paramInfo.incrDate, TimeUtils.YESTERDAY_FOLDER)
    val fraction = OptionUtils.getOptValue(paramInfo.fraction, DEFAULT_FRACTION).toDouble
    var status: Boolean = true
    // first load Common data sets
    val campaignList: List[BaseCampaignQuality] = List(ReturnReTargetQuality, CancelReTargetQuality, ACartPushCampaignQuality, WishlistCampaignQuality, InvalidFollowupQuality, InvalidLowStockQuality, Surf6Quality)
    loadCommonDataSets(incrDate)
    for (campaign <- campaignList) {
      if (!campaign.backwardTest(incrDate, fraction)) {
        logger.info(campaign.getName() + " failed for:-" + incrDate)
        status = false
      }
    }
   // if status is false means :- at least one of the campaign quality check has failed, Please check the log to actually see which campaign has failed
    if (status == false) {
      throw new FailedStatusException
    }
  }

  def loadCommonDataSets(date: String): Unit = {
    orderItemFullData = CampaignInput.loadFullOrderItemData(date)
    orderItem30DaysData = CampaignInput.loadLastNdaysOrderItemData(30, orderItemFullData, date).cache()
    orderItem3DaysData = CampaignInput.loadLastNdaysOrderItemData(3, orderItem30DaysData, date)
    orderItemData = CampaignInput.loadLastNdaysOrderItemData(1, orderItem3DaysData, date)
    fullOrderData = CampaignInput.loadFullOrderData(date)
    last30DaysOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData, date).cache()
    salesCart30DaysData = CampaignInput.loadLast30daysAcartData(date).cache()
    salesCart3rdDayData = CampaignInput.loadNthdayAcartData(3, salesCart30DaysData)
    yestSessionData = CampaignInput.loadYesterdaySurfSessionData()
  }

}

class FailedStatusException extends Exception