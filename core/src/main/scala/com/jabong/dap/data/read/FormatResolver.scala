package com.jabong.dap.data.read

import java.io.File
import scala.util.control.Breaks._

/**
 * Object to get check whether the format of data stored at a given directory
 * is orc or parquet.
 * WARNING: raises ValidFormatNotFound Exception in case orc or parquet file
 * is not found in the given directory.
 */
object FormatResolver {

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

  def getFileExtension(file: File): String = {
    val fileName = file.getName
    if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0) {
      fileName.substring(fileName.lastIndexOf(".") + 1)
    } else {
      null
    }
  }
}

class ValidFormatNotFound extends Exception
