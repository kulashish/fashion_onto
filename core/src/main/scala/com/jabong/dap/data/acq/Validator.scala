package com.jabong.dap.data.acq

import java.text.ParseException
import com.jabong.dap.common.Constants
import com.jabong.dap.data.acq.common.{ ImportInfo, TableInfo }
import com.jabong.dap.common.utils.Time._
/**
 * Created by Rachit on 15/6/15.
 */

case class ValidationException(message: String) extends Exception(message)

/**
 * Validator for the JSON file used for data acquisition.
 */
object Validator {

  def validateRequiredValues(table: TableInfo) = {
    require(table.source != null && table.source.length() != 0, "Source cannot be null or empty.")
    require(table.tableName != null && table.tableName.length() != 0, "Table name cannot be null or empty.")
    require(table.mode != null && table.mode.length() != 0, "Mode cannot be null or empty.")
    require(table.saveMode != null && table.saveMode.length() != 0, "Save mode cannot be null or empty.")
    require(table.saveFormat != null && table.saveFormat.length() != 0, "Save format cannot be null or empty.")
  }

  def validatePossibleValues(table: TableInfo) = {
    val possibleSources = Array("bob", "erp", "unicommerce", "nextbee")
    val possibleModes = Array("full", "daily", "hourly")
    val possibleSaveFormats = Array("orc", "parquet")
    val possibleSaveModes = Array("overwrite", "append", "ignore", "error")

    require(possibleSources.contains(table.source), "Source '%s' not recognized. Possible values: %s".format(table.source, possibleSources.mkString(",")))
    require(possibleModes.contains(table.mode), "Mode '%s' not recognized. Possible values: %s".format(table.mode, possibleModes.mkString(",")))
    require(possibleSaveFormats.contains(table.saveFormat), "Save format '%s' not recognized. Possible values: %s".format(table.saveFormat, possibleSaveFormats.mkString(",")))
    require(possibleSaveModes.contains(table.saveMode), "Save mode '%s' not recognized. Possible values: %s".format(table.saveMode, possibleSaveModes.mkString(",")))
  }

  def validateDateTimes(table: TableInfo) = {
    require(!(dateStringEmpty(table.rangeStart) ^ dateStringEmpty(table.rangeEnd)), "rangeStart and rangeEnd both should have values, or none of them should have a value")
  }

  def validateRanges(table: TableInfo) = {
    try {
      require(isStrictlyLessThan(table.rangeStart, table.rangeEnd), "Start date time should be strictly less than End date time")
    } catch {
      case e: ParseException => "Date should be of the format: %s".format(Constants.DateTimeFormat)
    }

    table.mode match {
      case "daily" =>
        require(isSameMonth(table.rangeStart, table.rangeEnd), "rangeFrom and rangeEnd must span only a single month for mode 'daily'. Please run multiple jobs if you " +
          "want data spanning multiple months.")
      case "hourly" =>
        require(table.mode == "hourly" && isSameDay(table.rangeStart, table.rangeEnd), "rangeFrom and rangeEnd must span only a single day for mode 'hourly'. Please run multiple jobs if you " +
          "want data spanning multiple days.")
      case _ =>
    }
  }

  def validate(info: ImportInfo) = {
    for (table <- info.acquisition) {
      validateRequiredValues(table)
      validatePossibleValues(table)
      validateDateTimes(table)
      if (!(dateStringEmpty(table.rangeStart) && dateStringEmpty(table.rangeEnd))) {
        validateRanges(table)
      }
    }
  }
}
