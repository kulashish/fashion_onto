package com.jabong.dap.common.time

import java.io.File

object TimeConstants {
  val CONVERT_MILLISECOND_TO_DAYS = 24 * 60 * 60 * 1000
  val DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val DATE_FORMAT = "yyyy-MM-dd"
  val DATE_FORMAT_FOLDER = "yyyy" + File.separator + "MM" + File.separator + "dd"
  val DATE_TIME_FORMAT_MS = "yyyy-MM-dd HH:mm:ss.SSS"
  val DATE_TIME_FORMAT_HRS_FOLDER = "yyyy" + File.separator + "MM" + File.separator + "dd" + File.separator + "HH"

  val START_TIME = "00:00:00"
  val END_TIME = "23:59:59"

  val START_MIN = "00:00"
  val END_MIN = "59:59"

  val YYYYMMDD = "yyyyMMdd"
  //used for day name
  val EEEE = "EEEE"

  val START_TIME_MS = "00:00:00.0"
  val END_TIME_MS = "23:59:59.9"

  val DD_MMM_YYYY_HH_MM_SS = "dd-MMM-yyyy HH:mm:ss"
}