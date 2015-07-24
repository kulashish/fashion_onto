package com.jabong.dap.model.custorder

import com.jabong.dap.data.acq.common.{ COVarInfo, COVarJobInfo }
import com.jabong.dap.data.storage.DataSets

/**
 * Created by pooja on 9/7/15.
 */
object COVarJsonValidator {
  def validateRequiredValues(coVarInfo: COVarInfo) = {
    require(coVarInfo.source != null && coVarInfo.source.length() != 0, "Source cannot be null or empty.")
    require(coVarInfo.saveMode != null && coVarInfo.saveMode.length() != 0, "Save mode cannot be null or empty.")
    require(coVarInfo.saveFormat != null && coVarInfo.saveFormat.length() != 0, "Save format cannot be null or empty.")
  }

  private def validatePossibleValues(coVarInfo: COVarInfo) = {
    val possibleSourceFormats = Array(DataSets.AD4PUSH)
    val possibleSaveFormats = Array(DataSets.ORC, DataSets.PARQUET)
    val possibleSaveModes = Array(DataSets.OVERWRITE_SAVEMODE, DataSets.APPEND_SAVEMODE, DataSets.IGNORE_SAVEMODE, DataSets.ERROR_SAVEMODE)

    require(possibleSourceFormats.contains(coVarInfo.source), "Source '%s' not recognized. Possible values: %s".
      format(coVarInfo.source, possibleSourceFormats.mkString(",")))
    require(possibleSaveFormats.contains(coVarInfo.saveFormat), "Save format '%s' not recognized. Possible values: %s".
      format(coVarInfo.saveFormat, possibleSaveFormats.mkString(",")))
    require(possibleSaveModes.contains(coVarInfo.saveMode), "Save mode '%s' not recognized. Possible values: %s".
      format(coVarInfo.saveMode, possibleSaveModes.mkString(",")))
  }

  def validate(info: COVarJobInfo) = {
    for (varInfo <- info.coVar) {
      validateRequiredValues(varInfo)
      validatePossibleValues(varInfo)
    }
  }

}
