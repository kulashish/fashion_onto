package com.jabong.dap.data.storage.merge.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path, FileSystem }
import com.jabong.dap.common.Spark
import java.io.File

/**
 * Verifies if the data exits at a given location.
 */
object DataVerifier {

  val hconf = Spark.getContext().hadoopConfiguration
  val hdfs = FileSystem.get(hconf)

  /**
   * Returns true if the file exists in the directory
   * @param directory directory to be checked.
   * @param fileName name of file to be checked.
   * @return true or false
   */
  def dataExists(directory: String, fileName: String): Boolean = {
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
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
  def hdfsDirExists(directory: String): Boolean = {
    hdfs.exists(new Path(directory))
  }

  /**
   * Returns true if it is able to remove the directory successfully.
   * @param directory directory to be checked.
   * @return true or false
   */
  def hdfsDirDelete(directory: String): Boolean = {
    hdfs.delete(new Path(directory), true)
  }
}
