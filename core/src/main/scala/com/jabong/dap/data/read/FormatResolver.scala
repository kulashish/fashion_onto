package com.jabong.dap.data.read

import java.io.File
import scala.util.control.Breaks._

object FormatResolver {

  /**
   * Returns the format (orc or parquet) in which the data is stored in the
   * given directory.
   * WARNING: raises ValidFormatNotFound Exception in case orc or parquet file
   * is not found in the given directory.
   */
  def getFormat(directory: String): String = {
    val dir = new File(directory)
    val directoryListing = dir.listFiles()
    var flag: String = null
    if (directoryListing != null) {
      breakable {
        for (file <- directoryListing) {
          val extension = getFileExtension(file)
          if (extension == "orc" || extension == "parquet") {
            flag = extension
            break()
          }
        }
      }
    } else {
      throw new ValidFormatNotFound
    }
    if (flag != null) {
      flag
    } else {
      throw new ValidFormatNotFound
    }
  }

  /**
   * Returns the extension of a file.
   */
  private def getFileExtension(file: File): String = {
    val fileName = file.getName
    if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0) {
      fileName.substring(fileName.lastIndexOf(".") + 1)
    } else {
      null
    }
  }
}

class ValidFormatNotFound extends Exception
