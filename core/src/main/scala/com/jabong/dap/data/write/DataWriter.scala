package com.jabong.dap.data.write

import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by pooja on 23/7/15.
 */
object DataWriter extends Logging {
  /**
   *
   * @param df
   * @param basePath
   * @param source
   * @param tableName
   * @param mode
   * @param date
   */
  def writeCsv(df: DataFrame, basePath: String, source: String, tableName: String, mode: String, date: String) {
    val writePath = PathBuilder.buildPath(basePath, source, tableName, mode, date)
    df.write.format("com.databricks.spark.csv").option("delimiter", ";").save(writePath)
  }

  /**
   *
   * @param df
   * @param basePath
   * @param source
   * @param tableName
   * @param mode
   * @param date
   */
  def writeParquet(df: DataFrame, basePath: String, source: String, tableName: String, mode: String, date: String, saveMode: String) {
    var diskMode = mode
    var reqDate = date

    if (mode.equals(DataSets.FULL_MERGE_MODE)) {
      diskMode = DataSets.FULL
      reqDate = "%s-%s".format(date, "24")
    }
    val writePath = PathBuilder.buildPath(basePath, source, tableName, diskMode, reqDate)
    if (canWrite(saveMode, writePath))
      df.write.parquet(writePath)
  }

  def canWrite(saveMode: String, savePath: String): Boolean = {
    if (saveMode.equals(DataSets.IGNORE_SAVEMODE)) {
      if (DataVerifier.dataExists(savePath)) {
        logger.info("File Already exists: " + savePath)
        println("File Already exists so not doing anything: " + savePath)
        return false
      }
      if (DataVerifier.dirExists(savePath)) {
        DataVerifier.dirDelete(savePath)
        logger.info("Directory with no success file was removed: " + savePath)
        println("Directory with no success file was removed: " + savePath)
      }
    }

    if (saveMode.equals(DataSets.ERROR_SAVEMODE) && DataVerifier.dirExists(savePath)) {
      logger.info("File Already exists and save Mode is error: " + savePath)
      println("File Already exists and save Mode is error: " + savePath)
      return false
    }

    true
  }

}
