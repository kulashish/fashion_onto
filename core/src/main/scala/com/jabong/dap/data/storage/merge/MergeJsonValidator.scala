package com.jabong.dap.data.storage.merge

import com.jabong.dap.data.acq.common.MergeJobInfo
import com.jabong.dap.data.acq.common.MergeInfo

object MergeJsonValidator {

  private def validateRequiredFields(mergeJob: MergeInfo) = {
    require(mergeJob.source != null && mergeJob.source.length() != 0, "Source cannot be null or empty.")
    require(mergeJob.tableName != null && mergeJob.tableName.length() != 0, "Table Name cannot be null or empty.")
    require(mergeJob.primaryKey != null && mergeJob.primaryKey.length() != 0, "Primary Key cannot be null or empty.")
    require(mergeJob.mergeMode != null && mergeJob.mergeMode.length() != 0, "Merge Mode cannot be null or empty.")
    require(mergeJob.saveMode != null && mergeJob.saveMode.length() != 0, "Save Mode cannot be null or empty.")
    require(mergeJob.saveFormat != null && mergeJob.saveFormat.length() != 0, "Save Format cannot be null or empty.")
  }

  private def validatePossibleValues(mergeJob: MergeInfo) = {
    val possibleSources = Array("bob", "erp", "unicommerce", "nextbee")
    val possibleMergeModes = Array("full")
    val possibleSaveFormats = Array("orc", "parquet")
    val possibleSaveModes = Array("overwrite", "append", "ignore", "error")

    require(possibleSources.contains(mergeJob.source), "Source '%s' not recognized. Possible values: %s".
      format(mergeJob.source, possibleSources.mkString(",")))
    require(possibleMergeModes.contains(mergeJob.mergeMode), "Mode '%s' not recognized. Possible values: %s".
      format(mergeJob.mergeMode, possibleMergeModes.mkString(",")))
    require(possibleSaveFormats.contains(mergeJob.saveFormat), "Save format '%s' not recognized. Possible values: %s".
      format(mergeJob.saveFormat, possibleSaveFormats.mkString(",")))
    require(possibleSaveModes.contains(mergeJob.saveMode), "Save mode '%s' not recognized. Possible values: %s".
      format(mergeJob.saveMode, possibleSaveModes.mkString(",")))

  }

  def validate(mergeInfo: MergeJobInfo) = {
    for (mergeJob <- mergeInfo.merge) {
      validateRequiredFields(mergeJob)
      validatePossibleValues(mergeJob)
    }

  }

}

