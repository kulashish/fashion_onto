package com.jabong.dap.model.customer

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{TimeUtils, TimeConstants}
import com.jabong.dap.data.acq.common.ParamInfo
import grizzled.slf4j.Logging

/**
 * Created by raghu on 17/8/15.
 */
object ContactListMobile extends Logging{

  def start(vars: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))
    val path = OptionUtils.getOptValue(vars.path)
    val saveMode = vars.saveMode
//    processData(prevDate, path, incrDate, saveMode)
  }

}
