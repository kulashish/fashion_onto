package com.jabong.dap.data.storage.merge.common

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Verifies if the data exits at a given location.
 */
object DataVerifier {

  val conf = new Configuration()
  val fileSystem = FileSystem.get(conf)

  /**
   * Returns true if the file exists in the directory
   * @param directory directory to be checked.
   * @param fileName name of file to be checked.
   * @return true or false
   */
  def dataExists(directory: String, fileName: String): Boolean = {
    val successFile = "%s%s%s".format(directory, File.separator, fileName)
    fileSystem.exists(new Path(successFile))
  }

  /**
   * Returns true if the _success file exists in the directory.
   * @param directory directory to be checked.
   * @return true or false
   */
  def dataExists(directory: String): Boolean = {
    dataExists(directory, "_SUCCESS")
  }

  /**
   * Returns true if the given directory exists else false.
   * @param directory directory to be checked.
   * @return true or false
   */
  def dirExists(directory: String): Boolean = {
    fileSystem.exists(new Path(directory))
  }

  /**
   * Returns true if it is able to remove the directory successfully.
   * @param directory directory to be checked.
   * @return true or false
   */
  def dirDelete(directory: String): Boolean = {
    fileSystem.delete(new Path(directory), true)
  }

  //TODO refactor and rename this file.
  def rename(src:String, dest: String) = {
    fileSystem.rename(new Path(src), new Path(dest))
  }
}
