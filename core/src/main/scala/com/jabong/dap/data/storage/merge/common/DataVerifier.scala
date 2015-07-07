package com.jabong.dap.data.storage.merge.common


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Verifies if the data exits at a given location.
 */
object DataVerifier {
  def hdfsDataExists(directory: String): Boolean = {
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val successFile = "%s_SUCCESS".format(directory)
    val successFilePath = new Path(successFile)
    fileSystem.exists(successFilePath)
  }
}
