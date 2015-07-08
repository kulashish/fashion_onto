package com.jabong.dap.data.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Verifies if the data exits at a given location.
 * Checks for _SUCCESS file if fileName is passed as null or empty.
 */
object DataVerifier {
  def hdfsDataExists(directory: String, fileName: String): Boolean = {
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val successFile = if (fileName == null || fileName == "") {
      "%s_SUCCESS".format(directory)
    } else {
      "%s%s".format(directory, fileName)
    }
    val successFilePath = new Path(successFile)
    fileSystem.exists(successFilePath)
  }
}
