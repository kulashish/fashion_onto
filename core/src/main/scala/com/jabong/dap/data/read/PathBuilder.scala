package com.jabong.dap.data.read

import java.io.File
import com.jabong.dap.common.AppConfig

object PathBuilder {

  /**
   * Builds the path for given source, tableName, mode and date.
   */
  def buildPath(source: String, tableName: String, mode: String, date: String): String = {
    val basePath = AppConfig.config.basePath
    val datePath = date.replaceAll("-", File.separator)
    "%s/%s/%s/%s/%s".format(basePath, source, tableName, mode, datePath)
  }
}
