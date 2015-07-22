package com.jabong.dap.common.time

import java.io.File

object Constants {
  val CONVERT_MILLISECOND_TO_DAYS = 24 * 60 * 60 * 1000
  val DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val DATE_FORMAT = "yyyy-MM-dd"
  val DATE_FORMAT_FOLDER = "yyyy" + File.separator + "MM" + File.separator + "dd"
  val DATE_TIME_FORMAT_MS = "yyyy-MM-dd HH:mm:ss.SSS"
  val DATE_TIME_FORMAT_HRS_FOLDER = "yyyy" + File.separator + "MM" + File.separator + "dd" + File.separator + "HH"

  val START_TIME = "00:00:00"
  val END_TIME = "23:59:59"

  val YYYYMMDD = "yyyyMMdd"
  //used for day name
  val EEEE = "EEEE"
}