package com.jabong.dap.model.custorder

import com.jabong.dap.data.acq.common.{ ParamJobInfo, ParamInfo }
import com.jabong.dap.data.storage.DataSets

/**
 * Created by pooja on 9/7/15.
 */
object ParamJsonValidator {
  def validateRequiredValues(paramInfo: ParamInfo) = {
    require(paramInfo.source != null && paramInfo.source.length() != 0, "Source cannot be null or empty.")
    require(paramInfo.saveMode != null && paramInfo.saveMode.length() != 0, "Save mode cannot be null or empty.")
    require(paramInfo.saveFormat != null && paramInfo.saveFormat.length() != 0, "Save format cannot be null or empty.")
  }

  private def validatePossibleValues(paramInfo: ParamInfo) = {
    val possibleSourceFormats = Array(DataSets.AD4PUSH, DataSets.CUSTOMER_DEVICE_MAPPING, DataSets.BASIC_ITR, DataSets.PRICING)
    val possibleSaveFormats = Array(DataSets.ORC, DataSets.PARQUET)
    val possibleSaveModes = Array(DataSets.OVERWRITE_SAVEMODE, DataSets.APPEND_SAVEMODE, DataSets.IGNORE_SAVEMODE, DataSets.ERROR_SAVEMODE)

    require(possibleSourceFormats.contains(paramInfo.source), "Source '%s' not recognized. Possible values: %s".
      format(paramInfo.source, possibleSourceFormats.mkString(",")))
    require(possibleSaveFormats.contains(paramInfo.saveFormat), "Save format '%s' not recognized. Possible values: %s".
      format(paramInfo.saveFormat, possibleSaveFormats.mkString(",")))
    require(possibleSaveModes.contains(paramInfo.saveMode), "Save mode '%s' not recognized. Possible values: %s".
      format(paramInfo.saveMode, possibleSaveModes.mkString(",")))
  }

  def validate(info: ParamJobInfo) = {
    for (paramInfo <- info.params) {
      validateRequiredValues(paramInfo)
      validatePossibleValues(paramInfo)
    }
  }

}
