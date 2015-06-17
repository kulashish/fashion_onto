package com.jabong.dap.data.acq

import java.text.{ ParseException, SimpleDateFormat }
import java.util.Calendar

import com.jabong.dap.common.Constants
import com.jabong.dap.data.acq.common.{ ImportInfo, TableInfo }

/**
 * Created by Rachit on 15/6/15.
 */

case class ValidationException(message: String) extends Exception(message)

/**
 * Validator for the JSON file used for data acquisition.
 */
object Validator {

  def validateRequiredValues(table: TableInfo) = {
    val message = {
      if (table.source == null || table.source.length() == 0) {
        "Source cannot be null or empty."
      } else if (table.tableName == null || table.tableName.length() == 0) {
        "Table name cannot be null or empty."
      } else if (table.mode == null || table.mode.length() == 0) {
        "Mode cannot be null or empty."
      } else if (table.saveMode == null || table.saveMode.length() == 0) {
        "Save mode cannot be null or empty."
      } else if (table.saveFormat == null || table.saveFormat.length() == 0) {
        "Save format cannot be null or empty."
      } else {
        ""
      }
    }

    if (message.length != 0) {
      throw ValidationException(message)
    }
  }

  def validatePossibleValues(table: TableInfo) = {
    val possibleSources = Array("bob", "erp", "unicommerce", "nextbee")
    val possibleModes = Array("full", "daily", "hourly")
    val possibleSaveFormats = Array("orc", "parquet")
    val possibleSaveModes = Array("overwrite", "append", "ignore", "error")

    val message = {
      if (!possibleSources.contains(table.source)) {
        "Source '%s' not recognized. Possible values: %s".format(table.source, possibleSources.mkString(","))
      } else if (!possibleModes.contains(table.mode)) {
        "Mode '%s' not recognized. Possible values: %s".format(table.mode, possibleModes.mkString(","))
      } else if (!possibleSaveFormats.contains(table.saveFormat)) {
        "Save format '%s' not recognized. Possible values: %s".format(table.saveFormat, possibleSaveFormats.mkString(","))
      } else if (!possibleSaveModes.contains(table.saveMode)) {
        "Save mode '%s' not recognized. Possible values: %s".format(table.saveMode, possibleSaveModes.mkString(","))
      } else {
        ""
      }
    }

    if (message.length != 0) {
      throw ValidationException(message)
    }
  }

  def validateDateTimes(table: TableInfo) = {
    val message = {
      if ((table.rangeStart == null || table.rangeStart.length() == 0) &&
        (table.rangeEnd == null || table.rangeEnd.length() == 0)) {
        ""
      } else if (table.rangeStart == null || table.rangeStart.length() == 0) {
        "rangeStart and rangeEnd both should have values, or none of them should have a value"
      } else if (table.rangeEnd == null || table.rangeEnd.length() == 0) {
        "rangeStart and rangeEnd both should have values, or none of them should have a value"
      } else {
        val format = new SimpleDateFormat(Constants.DateTimeFormat)
        try {
          val start = format.parse(table.rangeStart)
          val end = format.parse(table.rangeEnd)
          if (start.getTime >= end.getTime) {
            "Start date time should be strictly less than End date time"
          } else {
            ""
          }
        } catch {
          case e: ParseException => "Date should be of the format: %s".format(Constants.DateTimeFormat)
        }
      }
    }

    if (message.length != 0) {
      throw ValidationException(message)
    }
  }

  def validateRanges(table: TableInfo) = {
    val message = {
      val format = new SimpleDateFormat(Constants.DateTimeFormat)
      val start = Calendar.getInstance()
      val end = Calendar.getInstance()
      start.setTime(format.parse(table.rangeStart))
      end.setTime(format.parse(table.rangeEnd))
      if (table.mode == "daily") {
        if ((start.get(Calendar.YEAR) != end.get(Calendar.YEAR)) ||
          (start.get(Calendar.MONTH) != end.get(Calendar.MONTH))) {
          "rangeFrom and rangeEnd must span only a single month for mode 'daily'. Please run multiple jobs if you " +
            "want data spanning multiple months."
        } else {
          ""
        }
      } else if (table.mode == "hourly") {
        if ((start.get(Calendar.YEAR) != end.get(Calendar.YEAR)) ||
          (start.get(Calendar.MONTH) != end.get(Calendar.MONTH)) ||
          (start.get(Calendar.DATE) != end.get(Calendar.DATE))) {
          "rangeFrom and rangeEnd must span only a single day for mode 'hourly'. Please run multiple jobs if you " +
            "want data spanning multiple days."
        } else {
          ""
        }
      } else {
        ""
      }
    }

    if (message.length != 0) {
      throw ValidationException(message)
    }
  }

  def validate(info: ImportInfo) = {
    for (table <- info.acquisition) {
      validateRequiredValues(table)
      validatePossibleValues(table)
      validateDateTimes(table)
      if ((table.rangeStart != null && table.rangeStart.length() != 0) &&
        (table.rangeEnd != null && table.rangeEnd.length() != 0)) {
        validateRanges(table)
      }
    }
  }
}
