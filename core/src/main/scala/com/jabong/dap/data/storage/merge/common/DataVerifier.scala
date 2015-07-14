package com.jabong.dap.data.storage.merge.common


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Verifies if the data exits at a given location.
 */
object DataVerifier {
  /**
   * Returns true if the _success file exists in the directory given.
   * @param directory directory to be checked.
   * @return true or false
   */
  def hdfsDataExists(directory: String): Boolean = {
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val successFile = "%s_SUCCESS".format(directory)
    val successFilePath = new Path(successFile)
    fileSystem.exists(successFilePath)
  }

  /**
   * Returns true if the given directory exists else false.
   * @param directory directory to be checked.
   * @return true or false
   */
  def hdfsDirExists(directory: String): Boolean = {
    Files.exists(Paths.get(directory))
  }

  /**
   * Returns true if it is able to remove the directory successfully.
   * @param directory directory to be checked.
   * @return true or false
   */
  def hdfsDirDelete(directory: String): Boolean = {
    Files.deleteIfExists(Paths.get(directory))
  }
}
