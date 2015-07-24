package com.jabong.dap.data.read

import java.io.File

object PathBuilder {

  /**
   * Builds the path for given source, tableName, mode and date.
   */
  def buildPath(basePath: String, source: String, tableName: String, mode: String, date: String): String = {
    //here if Date has "-", it will get changed to File.separator.
    "%s/%s/%s/%s/%s".format(basePath, source, tableName, mode, date.replaceAll("-", File.separator))
  }
}
