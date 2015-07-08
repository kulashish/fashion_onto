package com.jabong.dap.data.read


object FormatResolver {

  /**
   * Gets the format of the data in which it is saved at a given path.
   */
  def resolveFormat (fetchPath: String): String = {
    if (DataVerifier.hdfsDataExists(fetchPath,"part-r-00001.gz.parquet")) {
      "parquet"
    } else if (DataVerifier.hdfsDataExists(fetchPath,  "part-r-00001.orc")) {
      "orc"
    } else {
      ""
    }
  }
}
