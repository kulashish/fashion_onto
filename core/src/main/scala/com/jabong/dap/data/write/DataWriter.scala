package com.jabong.dap.data.write

import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ SaveMode, DataFrame }

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
  def writeCsv(df: DataFrame, basePath: String, source: String, tableName: String, mode: String, date: String, header:String, delimeter: String) {
    val writePath = PathBuilder.buildPath(basePath, source, tableName, mode, date)
    if (canWrite(mode, writePath))
      df.coalesce(1).write.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimeter).save(writePath)
  }

  def getWritePath(basePath: String, source: String, tableName: String, mode: String, date: String): String = {
    var diskMode = mode
    var reqDate = date

    if (mode.equals(DataSets.FULL_MERGE_MODE)) {
      diskMode = DataSets.FULL
      reqDate = "%s-%s".format(date, "24")
    }
    PathBuilder.buildPath(basePath, source, tableName, diskMode, reqDate)
  }

  /**
   *
   * @param df
   * @param writePath
   */
  def writeParquet(df: DataFrame, writePath: String, saveMode: String) {
    df.write.mode(SaveMode.valueOf(saveMode)).parquet(writePath)
    println("Parquet Data written successfully to the following Path: " + writePath)
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
