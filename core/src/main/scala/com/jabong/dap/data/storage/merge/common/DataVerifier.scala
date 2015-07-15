package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.Spark
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * Verifies if the data exits at a given location.
 */
object DataVerifier {

  val hconf = Spark.getContext().hadoopConfiguration
  val hdfs = FileSystem.get(hconf)

  /**
   * Returns true if the _success file exists in the directory given.
   * @param directory directory to be checked.
   * @return true or false
   */
  def hdfsDataExists(directory: String): Boolean = {
    val successFile = "%s_SUCCESS".format(directory)
    hdfs.exists(new Path(successFile))
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
