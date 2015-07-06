package com.jabong.dap.data.storage.merge.common

import java.nio.file.{Paths, Files}

/**
 * Verifies if the data exits at a given location.
 */
object DataVerifier {
  def hdfsDataExists(directory: String) : Boolean = {
    val successFile = "%s_SUCCESS".format(directory)
    Files.exists(Paths.get(successFile))
  }
}
