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

      ReturnReTargetQuality.backwardTest(incrDate,OptionUtils.getOptValue(paramInfo.fraction,DEFAULT_FRACTION).toDouble)
      CancelReTargetQuality.backwardTest(incrDate,OptionUtils.getOptValue(paramInfo.fraction,DEFAULT_FRACTION).toDouble)
    }
}
