package com.jabong.dap.model.custorder

import com.jabong.dap.data.acq.common.{ VarJobInfo, VarInfo }
import com.jabong.dap.data.storage.DataSets

/**
 * Created by pooja on 9/7/15.
 */
object VarJsonValidator {
  def validateRequiredValues(varInfo: VarInfo) = {
    require(varInfo.source != null && varInfo.source.length() != 0, "Source cannot be null or empty.")
    require(varInfo.saveMode != null && varInfo.saveMode.length() != 0, "Save mode cannot be null or empty.")
    require(varInfo.saveFormat != null && varInfo.saveFormat.length() != 0, "Save format cannot be null or empty.")
  }

  private def validatePossibleValues(varInfo: VarInfo) = {
    val possibleSourceFormats = Array(DataSets.AD4PUSH, DataSets.CUSTOMER_DEVICE_MAPPING,DataSets.BASIC_ITR)
    val possibleSaveFormats = Array(DataSets.ORC, DataSets.PARQUET)
    val possibleSaveModes = Array(DataSets.OVERWRITE_SAVEMODE, DataSets.APPEND_SAVEMODE, DataSets.IGNORE_SAVEMODE, DataSets.ERROR_SAVEMODE)

    require(possibleSourceFormats.contains(varInfo.source), "Source '%s' not recognized. Possible values: %s".
      format(varInfo.source, possibleSourceFormats.mkString(",")))
    require(possibleSaveFormats.contains(varInfo.saveFormat), "Save format '%s' not recognized. Possible values: %s".
      format(varInfo.saveFormat, possibleSaveFormats.mkString(",")))
    require(possibleSaveModes.contains(varInfo.saveMode), "Save mode '%s' not recognized. Possible values: %s".
      format(varInfo.saveMode, possibleSaveModes.mkString(",")))
  }

  def validate(info: VarJobInfo) = {
    for (varInfo <- info.vars) {
      validateRequiredValues(varInfo)
      validatePossibleValues(varInfo)
    }
  }

}
