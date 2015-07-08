package com.jabong.dap.data.read

import com.jabong.dap.common.time._

/**
 * Created by Abhay on 7/7/15.
 */
object DateResolver {
  /**
   * Used to resolve the date to 'today's date' in case the date is not passed or is empty.
   */
  def resolveDate (date: String, dataType: String): String = {
    if (TimeUtils.dateStringEmpty(date)) {
      TimeUtils.getTodayDate(Constants.DATE_FORMAT)
    } else {
      date
    }
  }

  /**
   * Used to get most recent value of hour in hh format for which the data
   * is present for given input pathDate.
   * WARNING: throws DataNotFound exception if data is not found in any
   * value of hour for the given date.
   */
  def getDateHour (source: String, tableName: String, dataType: String, pathDate: String): String = {
    var hour: Int = 23
    var flag = 0
    var dateHour: String = null
    while ( hour >= 0 && flag ==0) {
      dateHour = withLeadingZeros(hour)
      val tempPath = PathBuilder.buildPath(source, tableName, dataType, pathDate)+ dateHour + "/"
      if (DataVerifier.hdfsDataExists(tempPath)) {
        flag = 1
      }
      hour = hour - 1
    }
    if (flag == 0) {
      throw new DataNotFound
    }
    dateHour
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
