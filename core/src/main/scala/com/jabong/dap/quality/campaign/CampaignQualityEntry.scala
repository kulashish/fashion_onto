package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by Kapil.Rajak on 14/8/15.
 */
object CampaignQualityEntry extends Logging {

  var orderItemFullData, orderItemData, orderItem30DaysData, orderItem3DaysData, fullOrderData, last30DaysOrderData, salesCart30DaysData, salesCart3rdDayData: DataFrame = null

  def start(paramInfo: ParamInfo) = {
    val DEFAULT_FRACTION = ".15"

    logger.info("Campaign Quality Triggered")
    val incrDate = OptionUtils.getOptValue(paramInfo.incrDate, TimeUtils.YESTERDAY_FOLDER)
    val fraction = OptionUtils.getOptValue(paramInfo.fraction, DEFAULT_FRACTION).toDouble
    var status: Boolean = true
    // first load Common data sets
    loadCommonDataSets(incrDate)
    if (!ReturnReTargetQuality.backwardTest(incrDate, fraction)) {
      logger.info("ReturnReTargetQuality failed for:-" + incrDate)
      status = false
    }
    if (!CancelReTargetQuality.backwardTest(incrDate, fraction)) {
      logger.info("CancelReTargetQuality failed for:-" + incrDate)
      status = false
    }
    if (!ACartPushCampaignQuality.backwardTest(incrDate, fraction)) {
      logger.info("ACartPushCampaignQuality failed for:-" + incrDate)
      status = false
    }
    if (!WishlistCampaignQuality.backwardTest(incrDate, fraction)) {
      logger.info("WishlistCampaignQuality failed for:-" + incrDate)
      status = false
    }
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
  }

}

class FailedStatusException extends Exception