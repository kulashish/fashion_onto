package com.jabong.dap.data.acq

import com.jabong.dap.data.acq.common.{ ImportInfo, TableInfo }
import com.jabong.dap.common.time.TimeUtils._
import com.jabong.dap.common.OptionUtils

/**
 * Validator for the JSON file used for data acquisition.
 */
object TablesJsonValidator {

  def validateRequiredValues(table: TableInfo) = {
    require(table.source != null && table.source.length() != 0, "Source cannot be null or empty.")
    require(table.tableName != null && table.tableName.length() != 0, "Table name cannot be null or empty.")
    require(table.mode != null && table.mode.length() != 0, "Mode cannot be null or empty.")
    require(table.saveMode != null && table.saveMode.length() != 0, "Save mode cannot be null or empty.")
    require(table.saveFormat != null && table.saveFormat.length() != 0, "Save format cannot be null or empty.")
  }

  def validatePossibleValues(table: TableInfo) = {
    val possibleSources = Array("bob", "erp", "unicommerce", "nextbee")
    val possibleModes = Array("full", "monthly", "daily", "hourly")
    val possibleSaveFormats = Array("orc", "parquet")
    val possibleSaveModes = Array("overwrite", "append", "ignore", "error")

    require(possibleSources.contains(table.source), "Source '%s' not recognized. Possible values: %s".
      format(table.source, possibleSources.mkString(",")))
    require(possibleModes.contains(table.mode), "Mode '%s' not recognized. Possible values: %s".
      format(table.mode, possibleModes.mkString(",")))
    require(possibleSaveFormats.contains(table.saveFormat), "Save format '%s' not recognized. Possible values: %s".
      format(table.saveFormat, possibleSaveFormats.mkString(",")))
    require(possibleSaveModes.contains(table.saveMode), "Save mode '%s' not recognized. Possible values: %s".
      format(table.saveMode, possibleSaveModes.mkString(",")))
  }

  def validateDateTimes(table: TableInfo, isHistory: Boolean) = {
    if (!isHistory) {
      require(
        !(OptionUtils.optStringEmpty(table.rangeStart) ^ OptionUtils.optStringEmpty(table.rangeEnd)),
        "rangeStart and rangeEnd both should have values, or none of them should have a value"
      )
    } else {
      require(
        !(OptionUtils.optStringEmpty(table.rangeStart)),
        "rangeStart should have value if we are trying to get historical data"
      )
    }

    // Check if rangeStart doesn't have a value for hourly mode.
    // rangeEnd doesn't need to be checked as it will have a value if rangeStart has a value.
    if (table.mode == "hourly") {
      require(
        !OptionUtils.optStringEmpty(table.rangeStart),
        "Range should be provided for hourly mode"
      )
    }
  }

  def validateRanges(rngStart: String, rngEnd: String, mode: String) = {
    require(
      isStrictlyLessThan(rngStart, rngEnd),
      "Start date time should be strictly less than End date time"
    )
    mode match {
      case "monthly" =>
        require(
          isSameYear(rngStart, rngEnd),
          "rangeFrom and rangeEnd must span only a single year for mode 'monthly'. Please run multiple jobs if you " +
            "want data spanning multiple years."
        )
      case "daily" =>
        require(
          isSameMonth(rngStart, rngEnd),
          "rangeFrom and rangeEnd must span only a single month for mode 'daily'. Please run multiple jobs if you " +
            "want data spanning multiple months."
        )
      case "hourly" =>
        require(
          isSameDay(rngStart, rngEnd),
          "rangeFrom and rangeEnd must span only a single day for mode 'hourly'. Please run multiple jobs if you " +
            "want data spanning multiple days."
        )
      case "full" =>
    }
  }

  def validate(info: ImportInfo) = {
    val isHistory = OptionUtils.getOptBoolVal(info.isHistory)
    for (table <- info.acquisition) {
      validateRequiredValues(table)
      validatePossibleValues(table)
      validateDateTimes(table, isHistory)
      if (!(OptionUtils.optStringEmpty(table.rangeStart) && OptionUtils.optStringEmpty(table.rangeEnd))) {
        validateRanges(table.rangeStart.orNull, table.rangeEnd.orNull, table.mode)
      }
    }
  }
}
