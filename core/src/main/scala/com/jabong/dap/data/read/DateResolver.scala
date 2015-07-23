package com.jabong.dap.data.read

import java.io.File

import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }

object DateResolver {

  /**
   * Appends the  most recent value of hour in hh format to the inputted date for which the data
   * exists for given input pathDate.
   * WARNING: throws DataNotFound exception if data is not found in any
   * value of hour for the given date.
   */
  def getDateWithHour(basePath: String, source: String, tableName: String, mode: String, date: String): String = {
    var hour: Int = 0
    var flag = 0
    var dateHour: String = null
    val nextDay = TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT, date)
    while (hour <= 23 && flag == 0) {
      dateHour = withLeadingZeros(hour)
      val path = PathBuilder.buildPath(basePath, source, tableName, mode, date) + File.separator + dateHour
      if (DataVerifier.dataExists(path)) {
        flag = 1
      }
      hour = hour + 1
    }
    if (flag == 0) {
      throw new DataNotFound
    }
    "%s-%s".format(nextDay, dateHour)
  }

  def withLeadingZeros(input: Int): String = {
    if (input < 10) {
      "0%s".format(input)
    } else {
      "%s".format(input)
    }
  }
}

class DataNotFound extends Exception
