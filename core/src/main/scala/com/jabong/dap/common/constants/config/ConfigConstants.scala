package com.jabong.dap.common.constants.config

import java.io.File

import com.jabong.dap.common.AppConfig

/**
 * Created by pooja on 11/8/15.
 */
object ConfigConstants {

  val basePath = AppConfig.config.basePath

  val INPUT_PATH = basePath + File.separator + "input"

  val TMP_PATH = basePath + File.separator + "tmp"

  val OUTPUT_PATH = AppConfig.config.outputPath
  //  val OUTPUT_PATH = OUTPUT_PATH = basePath + File.separator + "output"

}
