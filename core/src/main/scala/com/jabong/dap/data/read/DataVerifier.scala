package com.jabong.dap.data.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import java.io.File

/**
 * Verifies if the data exits at a given location by
 * checking for _SUCCESS file.
 */
object DataVerifier {
  def dataExists(directory: String): Boolean = {
    val fileSystem = FileSystem.get(new Configuration())
    val successFile = "%s%s_SUCCESS".format(directory, File.separator)
    val successFilePath = new Path(successFile)
    fileSystem.exists(successFilePath)
  }
}
