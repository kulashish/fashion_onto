package com.jabong.dap.data.read

import java.io.File

import com.jabong.dap.common.AppConfig

/**
 * Created by Abhay on 7/7/15.
 */
object PathBuilder {

  def buildPath (source: String, tableName: String, dataType: String, date: String): String = {
    val basePath = AppConfig.config.basePath
    val datePath = date.replaceAll("-", File.separator)
    "%s/%s/%s/%s/%s/".format(basePath, source, tableName, dataType, datePath)
  }



}
