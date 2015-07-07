package com.jabong.dap.data.read

import com.jabong.dap.common.time._

/**
 * Created by Abhay on 7/7/15.
 */
object DateResolver {

  def resolveDate (date: String, dataType: String): String = {
    if (TimeUtils.dateStringEmpty(date) == true){
      TimeUtils.getTodayDate(Constants.DATE_FORMAT)
    } else {
      date
    }
  }


  def getDateHour (source: String, tableName: String, dataType: String, pathDate: String): String = {
    var hour: Int = 23
    var flag = 0
    var dateHour: String = null
    while ( hour >= 0 && flag ==0) {
      dateHour = withLeadingZeros(hour)
      val tempPath = PathBuilder.buildPath(source, tableName, dataType, pathDate)+ dateHour + "/"
      if (DataVerifier.hdfsDataExists(tempPath) == true) {
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
