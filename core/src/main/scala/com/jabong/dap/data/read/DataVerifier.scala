package com.jabong.dap.data.read

import java.nio.file.{Paths, Files}

/**
 * Created by Abhay on 7/7/15.
 */
object DataVerifier {
  def hdfsDataExists(directory: String): Boolean = {
    val successFile = "%s_SUCCESS".format(directory)
    Files.exists(Paths.get(successFile))
  }


}
