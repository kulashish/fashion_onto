package com.jabong.dap.common.constants.config

import java.io.File

import com.jabong.dap.common.{ OptionUtils, AppConfig }

/**
 * Created by pooja on 11/8/15.
 */
object ConfigConstants {

  val basePath = AppConfig.config.basePath

  val INPUT_PATH = basePath + File.separator + "input"

  private val OUTPUT_PATH = AppConfig.config.basePath + File.separator + "output"

  val WRITE_OUTPUT_PATH = OptionUtils.getOptValue(AppConfig.config.writeOutputPath, OUTPUT_PATH)

  val TMP_PATH = WRITE_OUTPUT_PATH + File.separator + "tmp"

  val READ_OUTPUT_PATH = OptionUtils.getOptValue(AppConfig.config.readOutputPath, OUTPUT_PATH)

  val ZONE_CITY_PINCODE_PATH = basePath + File.separator + "input/DWH/zone_city_pincode/full/2015/09/24/ZONE_CITY_PINCODE_DWH_DATA_DUMP.csv"

  val ENV = AppConfig.config.env
}
