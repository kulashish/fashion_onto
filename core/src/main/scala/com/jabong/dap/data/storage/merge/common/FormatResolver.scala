package com.jabong.dap.data.storage.merge.common

/**
 * Gets the format of the data stored at hdfs location.
 */
object FormatResolver {

  def resolveFormat (fetchPath: String): String = {
    if (DataVerifier.hdfsDataExists(fetchPath,"part-r-00001.gz.parquet")) {
      "parquet"
    } else if (DataVerifier.hdfsDataExists(fetchPath,"part-r-00001.orc")) {
      "orc"
    } else {
      ""
    }
  }

}
