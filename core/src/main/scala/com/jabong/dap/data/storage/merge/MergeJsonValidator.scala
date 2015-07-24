package com.jabong.dap.data.storage.merge

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.data.acq.common.MergeJobInfo
import com.jabong.dap.data.acq.common.MergeInfo
import com.jabong.dap.data.storage.DataSets

/**
 * Validator for the JSON file used for data merge.
 */
object MergeJsonValidator {

  private def validateRequiredFields(mergeJob: MergeInfo) = {
    require(mergeJob.source != null && mergeJob.source.length() != 0, "Source cannot be null or empty.")
    require(mergeJob.tableName != null && mergeJob.tableName.length() != 0, "Table Name cannot be null or empty.")
    require(mergeJob.primaryKey != null && mergeJob.primaryKey.length() != 0, "Primary Key cannot be null or empty.")
    require(mergeJob.mergeMode != null && mergeJob.mergeMode.length() != 0, "Merge Mode cannot be null or empty.")
    require(mergeJob.saveMode != null && mergeJob.saveMode.length() != 0, "Save Mode cannot be null or empty.")
  }

  private def validatePossibleValues(mergeJob: MergeInfo) = {
    val possibleSources = Array(DataSets.BOB, DataSets.ERP, DataSets.UNICOMMERCE, DataSets.NEXTBEE)
    // Here the full mode will merge given date's incr and full file. And in case the incr date and full date is not
    // given then it will merge the yesterday's incr data with day before yesterday's full data.
    // Historical mode will merge starting from the incr date's till yesterday's data with the data of prevFullDate.
    // Here it will assume monthly data from the incr start date till last month and daily data for this month from 1st
    // to yesterda'y date.
    val possibleMergeModes = Array(DataSets.FULL, DataSets.HISTORICAL)
    val possibleSaveModes = Array(DataSets.OVERWRITE_SAVEMODE, DataSets.APPEND_SAVEMODE, DataSets.IGNORE_SAVEMODE, DataSets.ERROR_SAVEMODE)

    require(possibleSources.contains(mergeJob.source), "Source '%s' not recognized. Possible values: %s".
      format(mergeJob.source, possibleSources.mkString(",")))
    require(possibleMergeModes.contains(mergeJob.mergeMode), "Mode '%s' not recognized. Possible values: %s".
      format(mergeJob.mergeMode, possibleMergeModes.mkString(",")))
    require(possibleSaveModes.contains(mergeJob.saveMode), "Save mode '%s' not recognized. Possible values: %s".
      format(mergeJob.saveMode, possibleSaveModes.mkString(",")))

  }

  private def validateOptionalValues(mergeJob: MergeInfo) = {
    if (DataSets.MONTHLY_MODE.equals(mergeJob.mergeMode)) {
      val colName = OptionUtils.getOptValue(mergeJob.dateColumn)
      require(colName != null && colName.length() != 0, "Date Coulmn cannot be null or empty for %s mode".format(DataSets.MONTHLY_MODE))
    }
  }

  def validate(mergeInfo: MergeJobInfo) = {
    for (mergeJob <- mergeInfo.merge) {
      validateRequiredFields(mergeJob)
      validatePossibleValues(mergeJob)
      validateOptionalValues(mergeJob)
    }

  }

}

