package com.jabong.dap.common.constants.config

import java.io.File

import com.jabong.dap.common.{ OptionUtils, AppConfig }

/**
 * Created by pooja on 11/8/15.
 */
object ConfigConstants {

  val basePath = AppConfig.config.basePath

  val INPUT_PATH = basePath + File.separator + "input"

  val TMP_PATH = basePath + File.separator + "tmp"

  private val OUTPUT_PATH = AppConfig.config.basePath + File.separator + "output"

  val WRITE_OUTPUT_PATH = OptionUtils.getOptValue(AppConfig.config.writeOutputPath, OUTPUT_PATH)

  val READ_OUTPUT_PATH = OptionUtils.getOptValue(AppConfig.config.readOutputPath, OUTPUT_PATH)
}
