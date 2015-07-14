package com.jabong.dap.data.storage.merge.common

import java.nio.file.{ Paths, Files }

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
    val successFile = "%s_SUCCESS".format(directory)
    Files.exists(Paths.get(successFile))
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
