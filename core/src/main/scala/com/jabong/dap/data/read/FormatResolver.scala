package com.jabong.dap.data.read

import java.io.File
import com.jabong.dap.data.storage.DataSets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ LocatedFileStatus, RemoteIterator, Path, FileSystem }

import scala.util.control.Breaks._

object FormatResolver {

  /**
   * Returns the format (orc or parquet) in which the data is stored in the
   * given directory.
   * WARNING: raises ValidFormatNotFound Exception in case orc or parquet file
   * is not found in the given directory.
   */
  def getFormat(directory: String): String = {
    val fileSystem = FileSystem.get(new Configuration())
    val directoryListingIterator: RemoteIterator[LocatedFileStatus] = fileSystem.listFiles(new Path(directory), false)
    var flag: String = null
    if (directoryListingIterator.hasNext) {
      breakable {
        while (directoryListingIterator.hasNext) {
          val fileStatus: LocatedFileStatus = directoryListingIterator.next()
          val filename: String = fileStatus.getPath.getName
          val file = new File(filename)
          val extension = getFileExtension(file)
          if (extension == DataSets.ORC || extension == DataSets.PARQUET || extension == DataSets.CSV) {
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
