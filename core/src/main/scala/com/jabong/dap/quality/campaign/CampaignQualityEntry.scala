package com.jabong.dap.quality.campaign

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import grizzled.slf4j.Logging

/**
 * Created by Kapil.Rajak on 14/8/15.
 */
object CampaignQualityEntry extends Logging{

    def start(paramInfo: ParamInfo) ={
      val DEFAULT_FRACTION=".15"

      logger.info("Campaign Quality Triggered")
      val incrDate = OptionUtils.getOptValue(paramInfo.incrDate, TimeUtils.YESTERDAY_FOLDER)
      val fraction = OptionUtils.getOptValue(paramInfo.fraction,DEFAULT_FRACTION).toDouble
      var status: Boolean = true
      if(!ReturnReTargetQuality.backwardTest(incrDate, fraction)){
        logger.info("ReturnReTargetQuality failed for:-"+ incrDate)
        status = false
      }
      if(CancelReTargetQuality.backwardTest(incrDate, fraction)){
        logger.info("CancelReTargetQuality failed for:-"+ incrDate)
        status = false
      }
      if(ACartPushCampaignQuality.backwardTest(incrDate, fraction)){
        logger.info("ACartPushCampaignQuality failed for:-"+ incrDate)
        status = false
      }
      if(status == false) {
        throw new FailedStatusException
      }
    }

}

class FailedStatusException extends Exception