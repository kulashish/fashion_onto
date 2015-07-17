package com.jabong.dap.data.read

import java.io.File

object DateResolver {

  /**
   * Used to get most recent value of hour in hh format for which the data
   * exists for given input pathDate.
   * WARNING: throws DataNotFound exception if data is not found in any
   * value of hour for the given date.
   */
  def getDateWithHour(source: String, tableName: String, mode: String, pathDate: String): String = {
    var hour: Int = 23
    var flag = 0
    var dateHour: String = null
    while (hour >= 0 && flag == 0) {
      dateHour = withLeadingZeros(hour)
      val path = PathBuilder.buildPath(source, tableName, mode, pathDate) + File.separator + dateHour
      if (DataVerifier.dataExists(path)) {
        flag = 1
      }
      hour = hour - 1
    }
    if (flag == 0) {
      throw new DataNotFound
    }
    "%s-%s".format(pathDate, dateHour)
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
