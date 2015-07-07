package com.jabong.dap.data.read

/**
 * Created by Abhay on 7/7/15.
 */
object FormatResolver {
  def resolveFormat (fetchPath: String): String = {
    if (DataVerifier.hdfsDataExists(fetchPath + "part-r-00001.gz.parquet") == true) {
      "parquet"
    } else if (DataVerifier.hdfsDataExists(fetchPath + "part-r-00001.orc") == true) {
      "orc"
    } else {
      ""
    }
  }
}
