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
      logger.info("Campaign Quality Triggered")
      val incrDate = OptionUtils.getOptValue(paramInfo.incrDate, TimeUtils.YESTERDAY_FOLDER)

      ReturnReTargetQuality.backwardTest(incrDate,paramInfo.fraction.toDouble)
      CancelReTargetQuality.backwardTest(incrDate,paramInfo.fraction.toDouble)
    }
}
